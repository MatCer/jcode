//! Hydrate or register the subscribed session in its trusted swarm.
use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn ensure_client_swarm_member(
    client_session_id: &str,
    client_connection_id: &str,
    friendly_name: &Option<String>,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    agent: &Arc<Mutex<Agent>>,
    swarm_enabled: bool,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
) -> bool {
    let (working_dir, derived_swarm_id, fallback_name) = {
        // A target-aware subscribe can attach to an agent that is in the middle
        // of a turn. Never wait for that turn's agent lock just to populate
        // connection metadata: doing so prevents the subscribe request from
        // completing, so subsequent state requests sit unread until the desktop
        // client times out. The persisted startup stub has the same immutable
        // identity metadata and is safe to read while the live agent is busy.
        let (working_dir, fallback_name) = match agent.try_lock() {
            Ok(agent_guard) => (
                agent_guard.working_dir().map(PathBuf::from),
                agent_guard
                    .session_short_name()
                    .map(|value| value.to_string()),
            ),
            Err(_) => {
                crate::logging::info(&format!(
                    "Subscribe metadata for busy session {} is using the persisted startup stub",
                    client_session_id
                ));
                crate::session::Session::load_startup_stub(client_session_id)
                    .map(|session| (session.working_dir.map(PathBuf::from), session.short_name))
                    .unwrap_or((None, None))
            }
        };
        let derived_swarm_id = if swarm_enabled {
            swarm_id_for_session(client_session_id)
        } else {
            None
        };
        (working_dir, derived_swarm_id, fallback_name)
    };

    // Prefer the currently restored agent/session identity over the temporary
    // name captured at raw socket accept time. During resume/reconnect bursts,
    // the temporary pre-resume session name can otherwise leak onto the real
    // resumed session and corrupt swarm metadata.
    let member_name = fallback_name.or_else(|| friendly_name.clone());
    let mut inserted = false;
    {
        let mut members = swarm_members.write().await;
        if let Some(member) = members.get_mut(client_session_id) {
            member.event_tx = client_event_tx.clone();
            member
                .event_txs
                .insert(client_connection_id.to_string(), client_event_tx.clone());
            member.swarm_enabled = swarm_enabled;
            member.is_headless = false;
            if member_name.is_some() {
                member.friendly_name = member_name.clone();
            }
        } else {
            let now = Instant::now();
            members.insert(
                client_session_id.to_string(),
                SwarmMember {
                    session_id: client_session_id.to_string(),
                    event_tx: client_event_tx.clone(),
                    event_txs: HashMap::from([(
                        client_connection_id.to_string(),
                        client_event_tx.clone(),
                    )]),
                    working_dir: working_dir.clone(),
                    swarm_id: derived_swarm_id.clone(),
                    swarm_enabled,
                    status: "ready".to_string(),
                    detail: None,
                    task_label: None,
                    friendly_name: member_name.clone(),
                    report_back_to_session_id: None,
                    allowed_models: None,
                    latest_completion_report: None,
                    role: "agent".to_string(),
                    joined_at: now,
                    last_status_change: now,
                    is_headless: false,
                    output_tail: None,
                    todo_progress: None,
                    todo_items: Vec::new(),
                    runtime: crate::protocol::SwarmMemberRuntime::default(),
                },
            );
            inserted = true;
        }
    }

    if inserted && let Some(ref swarm_id_ref) = derived_swarm_id {
        let mut swarms = swarms_by_id.write().await;
        swarms
            .entry(swarm_id_ref.to_string())
            .or_insert_with(HashSet::new)
            .insert(client_session_id.to_string());
        drop(swarms);
        crate::server::record_swarm_event(
            event_history,
            event_counter,
            swarm_event_tx,
            client_session_id.to_string(),
            member_name,
            Some(swarm_id_ref.to_string()),
            crate::server::SwarmEventType::MemberChange {
                action: "joined".to_string(),
            },
        )
        .await;
    }

    crate::logging::event_info(
        "SESSION_LIFECYCLE",
        vec![
            ("phase", "swarm_member_registered".to_string()),
            ("session_id", client_session_id.to_string()),
            ("client_connection_id", client_connection_id.to_string()),
            ("inserted", inserted.to_string()),
            ("swarm_enabled", swarm_enabled.to_string()),
            (
                "swarm_id",
                derived_swarm_id.unwrap_or_else(|| "none".to_string()),
            ),
        ],
    );

    inserted
}
