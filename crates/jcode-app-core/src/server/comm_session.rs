use super::ClientConnectionInfo;
use super::client_lifecycle::process_message_streaming_mpsc;
use super::swarm_mutation_state::{
    PersistedSwarmMutationResponse, SwarmMutationRuntime, begin_or_replay,
    begin_or_replay_routed_spawn, finish_request, request_key,
};
use super::{
    SessionInterruptQueues, SwarmEvent, SwarmEventType, SwarmMember, SwarmState, VersionedPlan,
    append_swarm_completion_report_instructions, broadcast_swarm_plan, broadcast_swarm_status,
    fanout_session_event, persist_swarm_state_for, record_swarm_event,
    record_swarm_event_for_session, remove_background_tool_signal,
    remove_session_channel_subscriptions, remove_session_from_swarm,
    remove_session_interrupt_queue, set_member_task_label, truncate_detail, update_member_status,
    update_member_status_with_report,
};
use crate::agent::Agent;
use crate::config::SwarmSpawnMode;
use crate::protocol::{NotificationType, ServerEvent, SwarmRoutingStatus};
use crate::provider::Provider;
#[cfg(test)]
use crate::session::Session;
use jcode_plan::ResolvedExecution;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use tokio::sync::{Mutex, RwLock, broadcast, mpsc};

#[path = "comm_session/routing.rs"]
mod routing;
pub(super) use routing::*;

type SessionAgents = Arc<RwLock<HashMap<String, Arc<Mutex<Agent>>>>>;
type ChannelSubscriptions = Arc<RwLock<HashMap<String, HashMap<String, HashSet<String>>>>>;
type ClientConnections = Arc<RwLock<HashMap<String, ClientConnectionInfo>>>;

/// Serialize spawn admission through member registration within one swarm.
/// Without a reservation or lock, many recursive agents can all observe the same
/// free slot and burst past the configured limit before any child is registered.
/// Weak entries avoid retaining locks for swarms that are no longer active.
fn spawn_admission_lock(swarm_id: &str) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<StdMutex<HashMap<String, Weak<Mutex<()>>>>> = OnceLock::new();
    let locks = LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut locks = locks
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(swarm_id).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(swarm_id.to_string(), Arc::downgrade(&lock));
    lock
}

/// Look up the most recent terminal env snapshot for the live client connection
/// driving `session_id`, so spawn hooks target that client's terminal instead
/// of the long-lived server's stale startup env (#405). Prefers the most
/// recently seen connection when a session has more than one client attached.
async fn client_terminal_env_for_session(
    session_id: &str,
    client_connections: &ClientConnections,
) -> Vec<(String, String)> {
    let connections = client_connections.read().await;
    connections
        .values()
        .filter(|info| info.session_id == session_id && !info.terminal_env.is_empty())
        .max_by_key(|info| info.last_seen)
        .map(|info| info.terminal_env.clone())
        .unwrap_or_default()
}

#[cfg(test)]
fn create_visible_spawn_session(
    working_dir: Option<&str>,
    model_override: Option<&str>,
    provider_key_override: Option<&str>,
    route_api_method_override: Option<&str>,
    effort_override: Option<&str>,
    selfdev_requested: bool,
) -> anyhow::Result<(String, PathBuf)> {
    let cwd = working_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));

    // This legacy test-only preparation helper represents an explicitly
    // requested worker, not an untouched empty UI panel.
    let mut session = Session::create(None, None);
    session.title = Some("Prepared swarm worker".into());
    session.working_dir = Some(cwd.display().to_string());
    if let Some(model) = model_override {
        session.model = Some(model.to_string());
    }
    if let Some(provider_key) = provider_key_override {
        session.provider_key = Some(provider_key.to_string());
    }
    if let Some(route_api_method) = route_api_method_override
        .map(str::trim)
        .filter(|route| !route.is_empty())
    {
        session.route_api_method = Some(route_api_method.to_string());
    }
    if let Some(effort) = effort_override.map(str::trim).filter(|e| !e.is_empty()) {
        // Persisted effort is restored (and validated against the resolved
        // provider/model) by `restore_reasoning_effort_from_session` when the
        // headed client attaches to this session.
        session.reasoning_effort = Some(effort.to_string());
    }
    if selfdev_requested {
        session.set_canary("self-dev");
    }
    session.save()?;

    Ok((session.id.clone(), cwd))
}

async fn resolve_spawn_working_dir(
    requested_working_dir: Option<String>,
    req_session_id: &str,
    sessions: &SessionAgents,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
) -> Option<String> {
    if requested_working_dir
        .as_deref()
        .is_some_and(|dir| !dir.trim().is_empty())
    {
        return requested_working_dir;
    }

    if let Some(agent_dir) = {
        let agent_sessions = sessions.read().await;
        agent_sessions.get(req_session_id).and_then(|agent| {
            agent
                .try_lock()
                .ok()
                .and_then(|agent_guard| agent_guard.working_dir().map(str::to_string))
        })
    } && !agent_dir.trim().is_empty()
    {
        return Some(agent_dir);
    }

    swarm_members
        .read()
        .await
        .get(req_session_id)
        .and_then(|member| member.working_dir.as_ref())
        .map(|dir| dir.display().to_string())
        .filter(|dir| !dir.trim().is_empty())
}

/// Launch a headed window for `session_id`, exporting the given spawn context
/// (`JCODE_SPAWN_KIND`, swarm/coordinator ids, ...) to spawn hooks and
/// spawned terminals so external programs can reroute the window.
fn spawn_visible_session_window_with_context(
    session_id: &str,
    cwd: &std::path::Path,
    selfdev_requested: bool,
    provider_key: Option<&str>,
    context: &crate::session_launch::SessionSpawnContext,
) -> anyhow::Result<bool> {
    let exe = crate::build::client_update_candidate(selfdev_requested)
        .map(|(path, _label)| path)
        .or_else(|| std::env::current_exe().ok())
        .unwrap_or_else(|| PathBuf::from("jcode"));
    if selfdev_requested {
        crate::session_launch::spawn_selfdev_in_new_terminal_with_context(
            &exe,
            session_id,
            cwd,
            provider_key,
            context,
        )
    } else {
        crate::session_launch::spawn_resume_in_new_terminal_with_context(
            &exe,
            session_id,
            cwd,
            provider_key,
            context,
        )
    }
}

fn persist_headed_startup_message(session_id: &str, message: &str) {
    crate::logging::info(&format!(
        "Headed spawn: persisting startup submission for {session_id} (chars={}) to client-input handoff file",
        message.chars().count(),
    ));
    crate::client_input::save_startup_submission_for_session(
        session_id,
        message.to_string(),
        Vec::new(),
    );
}

fn clear_headed_startup_message(session_id: &str) {
    if let Ok(jcode_dir) = crate::storage::jcode_dir() {
        let path = jcode_dir.join(format!("client-input-{}", session_id));
        let _ = std::fs::remove_file(path);
    }
}

#[cfg(test)]
fn cleanup_prepared_visible_spawn_session(session_id: &str) {
    clear_headed_startup_message(session_id);
    if let Ok(path) = crate::session::session_path(session_id) {
        let _ = std::fs::remove_file(path);
    }
    if let Ok(path) = crate::session::session_journal_path(session_id) {
        let _ = std::fs::remove_file(path);
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
fn prepare_visible_spawn_session<F>(
    working_dir: Option<&str>,
    model_override: Option<&str>,
    provider_key_override: Option<&str>,
    route_api_method_override: Option<&str>,
    effort_override: Option<&str>,
    selfdev_requested: bool,
    startup_message: Option<&str>,
    launch_visible: F,
) -> anyhow::Result<(String, bool)>
where
    F: FnOnce(&str, &std::path::Path, bool, Option<&str>) -> anyhow::Result<bool>,
{
    let provider_key = provider_key_for_spawn_model(model_override, provider_key_override);
    let (new_session_id, cwd) = create_visible_spawn_session(
        working_dir,
        model_override,
        provider_key.as_deref(),
        route_api_method_override,
        effort_override,
        selfdev_requested,
    )?;

    if let Some(message) = startup_message {
        persist_headed_startup_message(&new_session_id, message);
    }

    match launch_visible(
        &new_session_id,
        &cwd,
        selfdev_requested,
        provider_key.as_deref(),
    ) {
        Ok(launched) => {
            if !launched {
                cleanup_prepared_visible_spawn_session(&new_session_id);
            }
            Ok((new_session_id, launched))
        }
        Err(error) => {
            cleanup_prepared_visible_spawn_session(&new_session_id);
            Err(error)
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "visible spawn registration updates swarm state, event history, and UI delivery metadata together"
)]
#[cfg(test)]
async fn register_visible_spawned_member(
    session_id: &str,
    swarm_id: &str,
    working_dir: Option<&str>,
    has_startup_message: bool,
    report_back_to_session_id: Option<&str>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
) {
    let (event_tx, _event_rx) = mpsc::unbounded_channel();
    let now = std::time::Instant::now();
    let friendly_name = crate::id::extract_session_name(session_id)
        .map(|name| name.to_string())
        .unwrap_or_else(|| session_id.to_string());
    let (status, detail) = if has_startup_message {
        ("running".to_string(), Some("startup queued".to_string()))
    } else {
        ("spawned".to_string(), Some("launching client".to_string()))
    };

    {
        let mut members = swarm_members.write().await;
        members.insert(
            session_id.to_string(),
            SwarmMember {
                session_id: session_id.to_string(),
                event_tx,
                event_txs: HashMap::new(),
                working_dir: working_dir.map(PathBuf::from),
                swarm_id: Some(swarm_id.to_string()),
                swarm_enabled: true,
                status,
                detail,
                task_label: None,
                friendly_name: Some(friendly_name),
                report_back_to_session_id: report_back_to_session_id.map(str::to_string),
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
    }

    {
        let mut swarms = swarms_by_id.write().await;
        swarms
            .entry(swarm_id.to_string())
            .or_insert_with(HashSet::new)
            .insert(session_id.to_string());
    }

    record_swarm_event_for_session(
        session_id,
        SwarmEventType::MemberChange {
            action: "joined".to_string(),
        },
        swarm_members,
        event_history,
        event_counter,
        swarm_event_tx,
    )
    .await;
    broadcast_swarm_status(swarm_id, swarm_members, swarms_by_id).await;
}

/// Resolve the reasoning effort for a spawned swarm worker (#1165).
///
/// Precedence mirrors the model path: an explicit `effort` on the spawn call
/// wins, then the `agents.swarm_effort` config pin, and only then does the
/// worker inherit the provider-wide reasoning effort (`None`).
pub(super) fn resolve_swarm_spawn_effort(
    requested_effort: Option<&str>,
    configured_swarm_effort: Option<&str>,
) -> Option<String> {
    let clean = |effort: Option<&str>| {
        effort
            .map(str::trim)
            .filter(|effort| !effort.is_empty())
            .map(str::to_string)
    };
    clean(requested_effort).or_else(|| clean(configured_swarm_effort))
}

#[expect(
    clippy::too_many_arguments,
    reason = "server-side swarm spawning needs session, swarm state, provider, and event sinks together"
)]
pub(super) async fn spawn_swarm_agent(
    req_session_id: &str,
    swarm_id: &str,
    working_dir: Option<String>,
    initial_message: Option<String>,
    spawn_mode: Option<SwarmSpawnMode>,
    requested_model: Option<String>,
    requested_effort: Option<String>,
    allowed_models: Option<Vec<String>>,
    pinned_execution: Option<ResolvedExecution>,
    label: Option<String>,
    sessions: &SessionAgents,
    global_session_id: &Arc<RwLock<String>>,
    provider_template: &Arc<dyn Provider>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
    swarm_plans: &Arc<RwLock<HashMap<String, VersionedPlan>>>,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
    mcp_pool: &Arc<crate::mcp::SharedMcpPool>,
    soft_interrupt_queues: &SessionInterruptQueues,
    client_connections: &ClientConnections,
) -> anyhow::Result<String> {
    let mut routing = admit_swarm_execution(
        req_session_id,
        crate::server::comm_session::SwarmExecutionRequest {
            requested_model: requested_model.clone(),
            requested_effort: requested_effort.clone(),
            requested_allowed: allowed_models.as_deref(),
            pinned: pinned_execution.as_ref(),
        },
        sessions,
        swarm_members,
        provider_template,
    )
    .await?;
    let initialized_provider = provider_template.fork();
    if let Some(target) = routing.resolved_execution.as_ref() {
        // Provider/auth failures occur outside the terminal-launch fallback.
        initialize_swarm_provider(initialized_provider.as_ref(), target, true)?;
    } else if let Some(effort) = requested_effort.as_deref() {
        initialized_provider.set_reasoning_effort(effort.trim())?;
        if initialized_provider.reasoning_effort().as_deref() != Some(effort.trim()) {
            anyhow::bail!("Explicit swarm effort was not applied.");
        }
    }
    let resolved_working_dir =
        resolve_spawn_working_dir(working_dir, req_session_id, sessions, swarm_members).await;
    let coordinator = resolve_coordinator_spawn_identity(req_session_id, sessions).await;
    let coordinator_is_canary = coordinator.is_canary;
    // Capture the requesting client's terminal env so spawn hooks place the new
    // window in the terminal the user is attached to, not the server's stale
    // startup env (#405).
    let client_terminal_env =
        client_terminal_env_for_session(req_session_id, client_connections).await;
    let agents_config = &crate::config::config().agents;
    let configured_swarm_model = agents_config.swarm_model.clone();
    let resolved_spawn_mode = spawn_mode.unwrap_or(agents_config.swarm_spawn_mode);
    let selection = routing
        .resolved_execution
        .as_ref()
        .map(|r| SwarmSpawnSelection {
            model: Some(r.model.clone()),
            provider_key: Some(r.provider_key.clone()),
            route_api_method: Some(r.api_method.clone()),
        })
        .unwrap_or_else(|| {
            resolve_swarm_spawn_selection(
                requested_model.clone(),
                configured_swarm_model.clone(),
                &coordinator,
            )
        });
    let spawn_model = selection.model.clone();
    let spawn_provider_key = selection.provider_key.clone();
    let spawn_route_api_method = selection.route_api_method.clone();
    let spawn_effort = match routing.resolved_execution.as_ref() {
        Some(target) => target.effort.clone(),
        None => resolve_swarm_spawn_effort(
            requested_effort.as_deref(),
            agents_config.swarm_effort.as_deref(),
        ),
    };
    crate::logging::info(&format!(
        "Swarm spawn model resolution: requested_model={:?} requested_effort={:?} configured_swarm_effort={:?} configured_swarm_model={:?} coordinator_model={:?} coordinator_provider_key={:?} coordinator_route={:?} -> spawn_model={:?} spawn_provider_key={:?} spawn_route={:?}",
        requested_model,
        requested_effort,
        agents_config.swarm_effort,
        configured_swarm_model,
        coordinator.model,
        coordinator.provider_key,
        coordinator.route_api_method,
        spawn_model,
        spawn_provider_key,
        spawn_route_api_method,
    ));

    let startup_message = initial_message
        .as_deref()
        .map(append_swarm_completion_report_instructions);

    // Prepare the real daemon-owned agent before launching a terminal. The
    // terminal attaches to this initialized session, not a request-only stub.
    // Failure to launch still uses this same verified agent headlessly.
    let command = resolved_working_dir
        .as_ref()
        .map(|dir| format!("create_session:{dir}"))
        .unwrap_or_else(|| "create_session".into());
    let state = SwarmState {
        members: Arc::clone(swarm_members),
        swarms_by_id: Arc::clone(swarms_by_id),
        plans: Arc::clone(swarm_plans),
        coordinators: Arc::clone(swarm_coordinators),
    };
    let created = super::headless::create_routed_headless_session(
        sessions,
        global_session_id,
        &initialized_provider,
        &state,
        soft_interrupt_queues,
        super::headless::HeadlessSessionOptions {
            command: &command,
            selfdev_requested: coordinator_is_canary,
            model_override: spawn_model.clone(),
            provider_key_override: spawn_provider_key.clone(),
            route_api_method_override: spawn_route_api_method.clone(),
            effort_override: spawn_effort.clone(),
            mcp_pool: Some(Arc::clone(mcp_pool)),
            report_back_to_session_id: Some(req_session_id.to_string()),
            memory_scope: super::headless::HeadlessMemoryScope::RealProject,
            routing: Some(routing.clone()),
        },
    )
    .await?;
    let created: serde_json::Value = serde_json::from_str(&created)?;
    let new_session_id = created
        .get("session_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Failed to parse spawned session id"))?
        .to_string();
    routing = swarm_members
        .read()
        .await
        .get(&new_session_id)
        .and_then(|m| m.runtime.routing.clone())
        .ok_or_else(|| {
            anyhow::anyhow!("Spawned session did not publish initialized routing status")
        })?;
    let visible_requested = matches!(
        resolved_spawn_mode,
        SwarmSpawnMode::Visible | SwarmSpawnMode::Auto
    );
    if visible_requested && let Some(member) = swarm_members.write().await.get_mut(&new_session_id)
    {
        member.is_headless = false;
        member.status = if startup_message.is_some() {
            "running"
        } else {
            "spawned"
        }
        .into();
        member.detail = Some(
            if startup_message.is_some() {
                "startup queued"
            } else {
                "launching client"
            }
            .into(),
        );
    }
    persist_swarm_state_for(swarm_id, &state).await;
    let launched = if visible_requested {
        if let Some(message) = startup_message.as_deref() {
            persist_headed_startup_message(&new_session_id, message);
        }
        let cwd = resolved_working_dir
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
        let context = crate::session_launch::SessionSpawnContext::kind("swarm-agent")
            .env("JCODE_SPAWN_SWARM_ID", swarm_id)
            .env("JCODE_SPAWN_COORDINATOR_SESSION_ID", req_session_id)
            .with_client_terminal_env(client_terminal_env);
        match spawn_visible_session_window_with_context(
            &new_session_id,
            &cwd,
            coordinator_is_canary,
            spawn_provider_key.as_deref(),
            &context,
        ) {
            Ok(launched) => launched,
            Err(error) => {
                crate::logging::info(&format!(
                    "Visible terminal unavailable; using verified headless worker: {error}"
                ));
                false
            }
        }
    } else {
        false
    };
    let is_headless_fallback = !launched;
    if !launched {
        clear_headed_startup_message(&new_session_id);
    }
    if let Some(member) = swarm_members.write().await.get_mut(&new_session_id) {
        member.is_headless = is_headless_fallback;
        member.allowed_models = routing.allowed_models.clone();
        member.runtime.routing = Some(routing);
        if !launched {
            member.status = "ready".into();
            member.detail = None;
        }
    }

    let startup_message = startup_message.clone();
    {
        let mut plans = swarm_plans.write().await;
        if let Some(plan) = plans.get_mut(swarm_id)
            && (!plan.items.is_empty() || !plan.participants.is_empty())
        {
            plan.participants.insert(req_session_id.to_string());
            plan.participants.insert(new_session_id.clone());
        }
    }

    broadcast_swarm_plan(
        swarm_id,
        Some("participant_spawned".to_string()),
        swarm_plans,
        swarm_members,
        swarms_by_id,
    )
    .await;
    // Label the worker with what it was spawned for so the swarm strip and
    // member lists can show the task, not just the animal name. An explicit
    // spawn `label` wins; otherwise the label is derived from the raw prompt
    // (before completion-report boilerplate is appended).
    if let Some(label_text) = label.as_deref().or(initial_message.as_deref()) {
        set_member_task_label(&new_session_id, label_text, swarm_members).await;
    }
    let swarm_state = SwarmState {
        members: Arc::clone(swarm_members),
        swarms_by_id: Arc::clone(swarms_by_id),
        plans: Arc::clone(swarm_plans),
        coordinators: Arc::clone(swarm_coordinators),
    };
    persist_swarm_state_for(swarm_id, &swarm_state).await;

    if let Some(initial_msg) = startup_message
        && is_headless_fallback
    {
        record_swarm_event_for_session(
            &new_session_id,
            SwarmEventType::MemberChange {
                action: "joined".to_string(),
            },
            swarm_members,
            event_history,
            event_counter,
            swarm_event_tx,
        )
        .await;

        let agent_arc = {
            let agent_sessions = sessions.read().await;
            agent_sessions.get(&new_session_id).cloned()
        };
        if let Some(agent_arc) = agent_arc {
            let sid_clone = new_session_id.clone();
            let swarm_members2 = Arc::clone(swarm_members);
            let swarms_by_id2 = Arc::clone(swarms_by_id);
            let event_history2 = Arc::clone(event_history);
            let event_counter2 = Arc::clone(event_counter);
            let swarm_event_tx2 = swarm_event_tx.clone();
            tokio::spawn(async move {
                update_member_status(
                    &sid_clone,
                    "running",
                    Some(truncate_detail(&initial_msg, 120)),
                    &swarm_members2,
                    &swarms_by_id2,
                    Some(&event_history2),
                    Some(&event_counter2),
                    Some(&swarm_event_tx2),
                )
                .await;
                let event_tx = super::session_event_fanout_sender(
                    sid_clone.clone(),
                    Arc::clone(&swarm_members2),
                );
                let start_message_index = {
                    let agent = agent_arc.lock().await;
                    agent.message_count()
                };
                let result = process_message_streaming_mpsc(
                    Arc::clone(&agent_arc),
                    &initial_msg,
                    vec![],
                    None,
                    event_tx,
                )
                .await;
                let completion_report = if result.is_ok() {
                    let agent = agent_arc.lock().await;
                    agent.latest_assistant_text_after(start_message_index)
                } else {
                    None
                };
                let (new_status, new_detail) = match result {
                    Ok(()) => ("ready", None),
                    Err(ref error) => ("failed", Some(truncate_detail(&error.to_string(), 120))),
                };
                update_member_status_with_report(
                    &sid_clone,
                    new_status,
                    new_detail,
                    completion_report,
                    &swarm_members2,
                    &swarms_by_id2,
                    Some(&event_history2),
                    Some(&event_counter2),
                    Some(&swarm_event_tx2),
                )
                .await;
            });
        }
    }

    Ok(new_session_id)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_comm_spawn(
    id: u64,
    req_session_id: String,
    working_dir: Option<String>,
    initial_message: Option<String>,
    request_nonce: Option<String>,
    spawn_mode: Option<SwarmSpawnMode>,
    model: Option<String>,
    effort: Option<String>,
    allowed_models: Option<Vec<String>>,
    label: Option<String>,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    sessions: &SessionAgents,
    global_session_id: &Arc<RwLock<String>>,
    provider_template: &Arc<dyn Provider>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
    swarm_plans: &Arc<RwLock<HashMap<String, VersionedPlan>>>,
    _channel_subscriptions: &ChannelSubscriptions,
    _channel_subscriptions_by_session: &ChannelSubscriptions,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
    mcp_pool: &Arc<crate::mcp::SharedMcpPool>,
    soft_interrupt_queues: &SessionInterruptQueues,
    swarm_mutation_runtime: &SwarmMutationRuntime,
    client_connections: &ClientConnections,
) {
    // Hold this swarm's admission through member registration so concurrent
    // recursive requests cannot all pass the population check against stale
    // state. Unrelated swarms retain independent spawn throughput.
    let admission_key = swarm_members
        .read()
        .await
        .get(&req_session_id)
        .and_then(|member| member.swarm_id.clone())
        .unwrap_or_else(|| req_session_id.clone());
    let admission_lock = spawn_admission_lock(&admission_key);
    let admission_guard = admission_lock.lock().await;
    if let Err(error) = admit_swarm_execution(
        &req_session_id,
        crate::server::comm_session::SwarmExecutionRequest {
            requested_model: model.clone(),
            requested_effort: effort.clone(),
            requested_allowed: allowed_models.as_deref(),
            pinned: None,
        },
        sessions,
        swarm_members,
        provider_template,
    )
    .await
    {
        if let Err(send_error) = client_event_tx.send(ServerEvent::Error {
            id,
            message: error.to_string(),
            retry_after_secs: None,
        }) {
            crate::logging::debug(&format!(
                "Spawn admission error delivery failed: {send_error}"
            ));
        }
        return;
    }
    let (allowed_fingerprint, routing_fingerprint) =
        match spawn_routing_fingerprints(&model, &effort, &allowed_models) {
            Ok(fingerprints) => fingerprints,
            Err(error) => {
                if let Err(send_error) = client_event_tx.send(ServerEvent::Error {
                    id,
                    message: error.to_string(),
                    retry_after_secs: None,
                }) {
                    crate::logging::debug(&format!(
                        "Spawn serialization error delivery failed: {send_error}"
                    ));
                }
                return;
            }
        };
    let swarm_id = match ensure_spawn_coordinator_swarm(
        id,
        &req_session_id,
        client_event_tx,
        swarm_members,
        swarms_by_id,
        swarm_coordinators,
        swarm_plans,
        crate::config::config().agents.swarm_max_concurrent_agents,
    )
    .await
    {
        Some(swarm_id) => swarm_id,
        None => return,
    };

    let mutation_key = request_key(
        &req_session_id,
        "spawn",
        &[
            swarm_id.clone(),
            working_dir.clone().unwrap_or_default(),
            initial_message.clone().unwrap_or_default(),
            request_nonce.clone().unwrap_or_default(),
            spawn_mode
                .map(|mode| format!("{mode:?}"))
                .unwrap_or_default(),
            effort.clone().unwrap_or_default(),
            model.clone().unwrap_or_default(),
            label.clone().unwrap_or_default(),
            allowed_fingerprint,
        ],
    );
    let mutation_state = if let Some(nonce) = request_nonce.as_ref() {
        let nonce_key = request_key(
            &req_session_id,
            "spawn-nonce",
            &[swarm_id.clone(), nonce.clone()],
        );
        begin_or_replay_routed_spawn(
            swarm_mutation_runtime,
            &nonce_key,
            &routing_fingerprint,
            &req_session_id,
            id,
            client_event_tx,
        )
        .await
    } else {
        begin_or_replay(
            swarm_mutation_runtime,
            &mutation_key,
            "spawn",
            &req_session_id,
            id,
            client_event_tx,
        )
        .await
    };
    let Some(mutation_state) = mutation_state else {
        return;
    };

    let response = match spawn_swarm_agent(
        &req_session_id,
        &swarm_id,
        working_dir,
        initial_message,
        spawn_mode,
        model,
        effort,
        allowed_models,
        None,
        label,
        sessions,
        global_session_id,
        provider_template,
        swarm_members,
        swarms_by_id,
        swarm_coordinators,
        swarm_plans,
        event_history,
        event_counter,
        swarm_event_tx,
        mcp_pool,
        soft_interrupt_queues,
        client_connections,
    )
    .await
    {
        Ok(new_session_id) => {
            let routing = swarm_members
                .read()
                .await
                .get(&new_session_id)
                .and_then(|m| m.runtime.routing.clone());
            PersistedSwarmMutationResponse::Spawn {
                new_session_id,
                routing,
            }
        }
        Err(error) => PersistedSwarmMutationResponse::Error {
            message: format!("Failed to spawn agent: {error}"),
            retry_after_secs: None,
        },
    };

    // The new member is registered (or spawning failed), so the next admission
    // check can safely observe the updated population.
    drop(admission_guard);

    finish_request(swarm_mutation_runtime, &mutation_state, response).await;
}

/// Handle `comm_list_models`: report the model routes available for spawning
/// swarm agents, plus the requester's current model (the spawn default) and
/// any `agents.swarm_model` config pin. Read-only, so it needs no coordinator
/// check or mutation dedup. Uses the requester's live agent catalog when its
/// lock is free, otherwise falls back to the provider template's catalog.
pub(super) async fn handle_comm_list_models(
    id: u64,
    req_session_id: &str,
    sessions: &SessionAgents,
    provider_template: &Arc<dyn Provider>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    send_event: impl FnOnce(ServerEvent),
) {
    let coordinator = resolve_coordinator_spawn_identity(req_session_id, sessions).await;

    let agent = {
        let agent_sessions = sessions.read().await;
        agent_sessions.get(req_session_id).cloned()
    };
    let model_routes = match agent.as_ref().and_then(|agent| agent.try_lock().ok()) {
        Some(agent_guard) => agent_guard.model_routes(),
        // Agent busy (mid-turn, the common case for tool calls) or not
        // resident: the provider template exposes the same route catalog.
        None => provider_template.model_routes(),
    };

    let config = &crate::config::config().agents;
    let allowed = match effective_swarm_allowed_models(
        &*swarm_members.read().await,
        req_session_id,
        config
            .swarm_model_selection
            .then_some(config.swarm_allowed_models.as_deref())
            .flatten(),
        None,
        &model_routes,
    ) {
        Ok(allowed) => allowed,
        Err(error) => {
            send_event(ServerEvent::Error {
                id,
                message: error.to_string(),
                retry_after_secs: None,
            });
            return;
        }
    };
    let actual_execution = match agent.as_ref() {
        Some(agent) => match agent.try_lock() {
            Ok(agent) => observe_optional_swarm_execution(agent.provider_handle().as_ref()),
            // A busy agent cannot attest current identity. Never use cached actual.
            Err(_) => None,
        },
        None => None,
    };
    let routing = Some(SwarmRoutingStatus {
        selection_enabled: Some(config.swarm_model_selection),
        allowed_models: allowed,
        actual_execution,
        ..Default::default()
    });
    send_event(ServerEvent::CommListModelsResponse {
        id,
        routing,
        current_model: coordinator.model,
        configured_swarm_model: crate::config::config().agents.swarm_model.clone(),
        model_routes,
    });
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_comm_stop(
    id: u64,
    req_session_id: String,
    target_session: String,
    force: bool,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    sessions: &SessionAgents,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
    swarm_plans: &Arc<RwLock<HashMap<String, VersionedPlan>>>,
    channel_subscriptions: &ChannelSubscriptions,
    channel_subscriptions_by_session: &ChannelSubscriptions,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
    soft_interrupt_queues: &SessionInterruptQueues,
    swarm_mutation_runtime: &SwarmMutationRuntime,
) {
    // Stopping is authorized per-target by ownership (the requester is the
    // target's spawner or a transitive ancestor) rather than by the swarm-level
    // coordinator slot, so that any parent can stop agents in its own subtree.
    // We only require the requester to be a member of a swarm here; the concrete
    // permission check happens below via `stop_allowed`.
    let swarm_id = {
        let members = swarm_members.read().await;
        members
            .get(&req_session_id)
            .and_then(|member| member.swarm_id.clone())
    };
    let Some(swarm_id) = swarm_id else {
        let _ = client_event_tx.send(ServerEvent::Error {
            id,
            message: "Not in a swarm.".to_string(),
            retry_after_secs: None,
        });
        return;
    };

    let target_session =
        match resolve_stop_target_session(&swarm_id, &target_session, swarm_members).await {
            Ok(target_session) => target_session,
            Err(message) => {
                let _ = client_event_tx.send(ServerEvent::Error {
                    id,
                    message,
                    retry_after_secs: None,
                });
                return;
            }
        };

    let stop_allowed = {
        let members = swarm_members.read().await;
        members
            .get(&target_session)
            .map(|member| {
                swarm_stop_allowed_by_owner(&req_session_id, member, force)
                    || (!force
                        && super::swarm_is_self_or_ancestor(
                            &members,
                            &req_session_id,
                            &target_session,
                        ))
            })
            .unwrap_or(false)
    };
    if !stop_allowed {
        let _ = client_event_tx.send(ServerEvent::Error {
            id,
            message: format!(
                "Refusing to stop session '{target_session}' because it was not spawned by this coordinator. Pass force=true to stop a non-owned/user-created swarm session explicitly."
            ),
            retry_after_secs: None,
        });
        return;
    }

    let _ = fanout_session_event(
        swarm_members,
        &target_session,
        ServerEvent::SessionCloseRequested {
            reason: format!("Stopped by coordinator {req_session_id}"),
        },
    )
    .await;

    let mutation_key = request_key(&req_session_id, "stop", &[swarm_id, target_session.clone()]);
    let Some(mutation_state) = begin_or_replay(
        swarm_mutation_runtime,
        &mutation_key,
        "stop",
        &req_session_id,
        id,
        client_event_tx,
    )
    .await
    else {
        return;
    };

    let removed_agent = super::remove_session_entry(sessions, &target_session).await;
    let removed_live_agent = removed_agent.is_some();
    if let Some(agent_arc) = removed_agent {
        remove_session_interrupt_queue(soft_interrupt_queues, &target_session).await;
        remove_background_tool_signal(&target_session);
        if let Ok(mut agent) = agent_arc.try_lock() {
            agent.mark_closed();
            let memory_enabled = agent.memory_enabled();
            let transcript = if memory_enabled {
                Some(agent.build_transcript_for_extraction())
            } else {
                None
            };
            let sid = target_session.clone();
            let working_dir = agent.working_dir().map(|dir| dir.to_string());
            drop(agent);
            if let Some(transcript) = transcript {
                crate::memory_agent::trigger_final_extraction_with_dir(
                    transcript,
                    sid,
                    working_dir,
                );
            }
        }
    }

    let (removed_swarm_id, removed_name) = {
        let mut members = swarm_members.write().await;
        if let Some(member) = members.remove(&target_session) {
            (member.swarm_id, member.friendly_name)
        } else {
            (None, None)
        }
    };
    if let Some(ref swarm_id) = removed_swarm_id {
        record_swarm_event(
            event_history,
            event_counter,
            swarm_event_tx,
            target_session.clone(),
            removed_name.clone(),
            Some(swarm_id.clone()),
            SwarmEventType::MemberChange {
                action: "left".to_string(),
            },
        )
        .await;
        remove_session_from_swarm(
            &target_session,
            swarm_id,
            swarm_members,
            swarms_by_id,
            swarm_coordinators,
            swarm_plans,
        )
        .await;
    }
    remove_session_channel_subscriptions(
        &target_session,
        channel_subscriptions,
        channel_subscriptions_by_session,
    )
    .await;

    let response = if removed_live_agent || removed_swarm_id.is_some() {
        PersistedSwarmMutationResponse::Done
    } else {
        PersistedSwarmMutationResponse::Error {
            message: format!("Unknown session '{target_session}'"),
            retry_after_secs: None,
        }
    };
    finish_request(swarm_mutation_runtime, &mutation_state, response).await;
}

fn swarm_stop_allowed_by_owner(
    req_session_id: &str,
    target_member: &SwarmMember,
    force: bool,
) -> bool {
    force || target_member.report_back_to_session_id.as_deref() == Some(req_session_id)
}

async fn resolve_stop_target_session(
    swarm_id: &str,
    target: &str,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
) -> std::result::Result<String, String> {
    let target = target.trim();
    if target.is_empty() {
        return Err("target_session is required.".to_string());
    }

    let members = swarm_members.read().await;
    if members
        .get(target)
        .is_some_and(|member| member.swarm_id.as_deref() == Some(swarm_id))
    {
        return Ok(target.to_string());
    }

    let mut matches = members
        .iter()
        .filter(|(_, member)| member.swarm_id.as_deref() == Some(swarm_id))
        .filter(|(session_id, member)| {
            member.friendly_name.as_deref() == Some(target)
                || session_id.starts_with(target)
                || session_id.ends_with(target)
        })
        .map(|(session_id, member)| {
            (
                session_id.clone(),
                member
                    .friendly_name
                    .as_deref()
                    .unwrap_or(session_id)
                    .to_string(),
            )
        })
        .collect::<Vec<_>>();
    matches.sort_by(|a, b| a.0.cmp(&b.0));

    match matches.len() {
        0 => Err(format!(
            "Unknown swarm session '{target}'. Use an exact session ID, unique friendly name, or unique session ID prefix/suffix."
        )),
        1 => Ok(matches.remove(0).0),
        _ => Err(format!(
            "Ambiguous swarm session '{target}' matched: {}. Use an exact session ID.",
            matches
                .iter()
                .map(|(session_id, friendly)| format!("{friendly} [{session_id}]"))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn swarm_member_status_is_stale_for_coordination(status: &str) -> bool {
    matches!(
        status,
        "crashed" | "failed" | "stopped" | "closed" | "disconnected"
    )
}

#[allow(clippy::too_many_arguments)]
async fn ensure_spawn_coordinator_swarm(
    id: u64,
    req_session_id: &str,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
    swarm_plans: &Arc<RwLock<HashMap<String, VersionedPlan>>>,
    configured_live_agent_limit: usize,
) -> Option<String> {
    let (
        swarm_id,
        from_name,
        is_root,
        root_session_id,
        coordinator_id,
        coordinator_is_stale,
        live_member_count,
        live_spawned_agent_count,
    ) = {
        let members = swarm_members.read().await;
        let swarm_id = members
            .get(req_session_id)
            .and_then(|member| member.swarm_id.clone());
        let from_name = members
            .get(req_session_id)
            .and_then(|member| member.friendly_name.clone());
        // A session is a "root" when it has no spawner/owner above it.
        let is_root = members
            .get(req_session_id)
            .and_then(|member| member.report_back_to_session_id.clone())
            .is_none();
        let root_session_id = super::swarm::swarm_ancestors(&members, req_session_id)
            .last()
            .cloned()
            .unwrap_or_else(|| req_session_id.to_string());
        // Count both all live members for the absolute hard cap and live spawned
        // agents for the configurable RAM-safety cap. User-created roots do not
        // consume worker slots; every recursively spawned descendant does.
        let (live_member_count, live_spawned_agent_count) = swarm_id
            .as_ref()
            .map(|swarm_id| {
                members
                    .values()
                    .filter(|member| member.swarm_id.as_deref() == Some(swarm_id.as_str()))
                    .filter(|member| super::member_consumes_swarm_capacity(member))
                    .fold((0usize, 0usize), |(members, spawned), member| {
                        (
                            members + 1,
                            spawned + usize::from(member.report_back_to_session_id.is_some()),
                        )
                    })
            })
            .unwrap_or_default();
        let coordinator_id = if let Some(ref swarm_id) = swarm_id {
            let coordinators = swarm_coordinators.read().await;
            coordinators.get(swarm_id).cloned()
        } else {
            None
        };
        let coordinator_is_stale = coordinator_id.as_ref().is_some_and(|coordinator| {
            !members.get(coordinator).is_some_and(|member| {
                // A coordinator is stale for slot-reclaim purposes when it left
                // the swarm, reached a terminal status, or can no longer be
                // reached at all (every event channel closed). The last case
                // catches a wedged coordinator whose client died without a
                // clean status transition; without it the slot stays blocked
                // until the status sweep happens to notice.
                let unreachable = member.event_tx.is_closed()
                    && member.event_txs.values().all(|tx| tx.is_closed());
                member.swarm_id.as_deref() == swarm_id.as_deref()
                    && !swarm_member_status_is_stale_for_coordination(&member.status)
                    && !unreachable
            })
        });
        (
            swarm_id,
            from_name,
            is_root,
            root_session_id,
            coordinator_id,
            coordinator_is_stale,
            live_member_count,
            live_spawned_agent_count,
        )
    };

    let Some(swarm_id) = swarm_id else {
        let _ = client_event_tx.send(ServerEvent::Error {
            id,
            message: "Not in a swarm.".to_string(),
            retry_after_secs: None,
        });
        return None;
    };

    // Light and ad hoc swarms are deliberately one-level fan-out: only the root
    // session may create workers. Recursive spawning is an explicit deep-swarm
    // capability, keyed from the root's effort rather than the requesting
    // child's effort so a worker cannot opt itself into unbounded growth.
    if !is_root {
        let root_is_deep = crate::session_effort::session_effort(&root_session_id)
            .as_deref()
            .is_some_and(crate::prompt::is_deep_swarm_effort);
        if !root_is_deep {
            let _ = client_event_tx.send(ServerEvent::Error {
                id,
                message: format!(
                    "Recursive swarm spawning is disabled for light and ad hoc swarms. Only the root session ({root_session_id}) may spawn agents unless that root is running in swarm-deep mode."
                ),
                retry_after_secs: None,
            });
            return None;
        }
    }

    // Keep an absolute hard ceiling even when the configurable limit is disabled.
    if live_member_count >= super::MAX_SWARM_MEMBERS {
        let _ = client_event_tx.send(ServerEvent::Error {
            id,
            message: format!(
                "Swarm member limit reached (hard max {}). This swarm already has {live_member_count} live members; it cannot spawn more. Let existing agents finish and free up capacity, or narrow the task decomposition before spawning further.",
                super::MAX_SWARM_MEMBERS
            ),
            retry_after_secs: None,
        });
        return None;
    }

    // `swarm_max_concurrent_agents` is the machine-safety budget shared by
    // run_plan and deep recursive spawning. Previously only run_plan obeyed it,
    // so nested agents could grow to the 1000-member hard cap and exhaust RAM.
    let live_agent_limit = (configured_live_agent_limit > 0)
        .then(|| configured_live_agent_limit.min(super::MAX_SWARM_MEMBERS));
    if live_agent_limit.is_some_and(|limit| live_spawned_agent_count >= limit) {
        let limit = live_agent_limit.unwrap_or(super::MAX_SWARM_MEMBERS);
        let _ = client_event_tx.send(ServerEvent::Error {
            id,
            message: format!(
                "Swarm live-agent limit reached (max {limit}, configured by agents.swarm_max_concurrent_agents). This swarm already has {live_spawned_agent_count} active spawned agents. Let existing agents finish or stop them before spawning more."
            ),
            retry_after_secs: None,
        });
        return None;
    }

    // Coordinator-slot election is now only about the swarm-level coordinator used
    // for shared plan operations (propose/approve/assign). Only a root session
    // (depth 0, no spawner) claims it, and only when the slot is empty or stale.
    // Authorized deep-swarm descendants coordinate their own subtree via
    // report-back ownership and never disturb the swarm-level coordinator slot.
    if is_root && coordinator_id.as_deref() != Some(req_session_id) {
        let should_claim = coordinator_id.is_none() || coordinator_is_stale;
        if should_claim {
            let promoted = {
                let mut coordinators = swarm_coordinators.write().await;
                match coordinators.get(&swarm_id) {
                    Some(existing) if existing == req_session_id => false,
                    Some(_) if !coordinator_is_stale => false,
                    _ => {
                        coordinators.insert(swarm_id.clone(), req_session_id.to_string());
                        true
                    }
                }
            };

            if promoted {
                {
                    let mut members = swarm_members.write().await;
                    if let Some(member) = members.get_mut(req_session_id) {
                        member.role = "coordinator".to_string();
                    }
                }
                let swarm_state = SwarmState {
                    members: Arc::clone(swarm_members),
                    swarms_by_id: Arc::clone(swarms_by_id),
                    plans: Arc::clone(swarm_plans),
                    coordinators: Arc::clone(swarm_coordinators),
                };
                persist_swarm_state_for(&swarm_id, &swarm_state).await;
                broadcast_swarm_status(&swarm_id, swarm_members, swarms_by_id).await;
                let _ = client_event_tx.send(ServerEvent::Notification {
                    from_session: req_session_id.to_string(),
                    from_name,
                    notification_type: NotificationType::Message {
                        scope: Some("swarm".to_string()),
                        channel: None,
                        tldr: None,
                    },
                    message: "You are the coordinator for this swarm.".to_string(),
                });
            }
        }
    }

    Some(swarm_id)
}

#[cfg(test)]
#[path = "comm_session_tests.rs"]
mod comm_session_tests;
