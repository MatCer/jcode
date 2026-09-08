//! Turn dispatch holds the agent guard from routing validation into execution.
use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn start_processing_message(
    message: ProcessingMessage,
    client_session_id: &str,
    state: &mut ProcessingState<'_>,
    agent: &Arc<Mutex<Agent>>,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    processing_done_tx: &mpsc::UnboundedSender<(u64, Result<()>, Option<String>)>,
    client_terminal_env: Vec<(String, String)>,
    swarm: &SwarmStatusRefs<'_>,
) {
    let ProcessingMessage {
        id,
        content,
        images,
        system_reminder,
        active_skill,
    } = message;
    if server_reload_starting() {
        crate::logging::info(&format!(
            "Rejecting new message for session {} because server reload is starting",
            client_session_id
        ));
        let _ = client_event_tx.send(ServerEvent::Reloading { new_socket: None });
        return;
    }

    if *state.client_is_processing {
        let _ = client_event_tx.send(ServerEvent::Error {
            id,
            message: "Already processing a message".to_string(),
            retry_after_secs: None,
        });
        return;
    }

    if !agent
        .lock()
        .await
        .set_remote_active_skill(active_skill.clone())
    {
        let skill_name = active_skill.as_deref().unwrap_or_default();
        let _ = client_event_tx.send(ServerEvent::Error {
            id,
            message: format!("Skill '{skill_name}' is not installed on the server"),
            retry_after_secs: None,
        });
        return;
    }

    *state.client_is_processing = true;
    *state.message_id = Some(id);
    *state.session_id = Some(client_session_id.to_string());

    if let Some(reminder) = system_reminder.as_deref()
        && let Err(error) = crate::server::reload_recovery::mark_delivered_if_matching_continuation(
            client_session_id,
            reminder,
            "client_message_accepted",
        )
    {
        crate::logging::warn(&format!(
            "Failed to mark reload recovery intent delivered for accepted message session={} id={}: {}",
            client_session_id, id, error
        ));
    }

    update_member_status(
        client_session_id,
        "running",
        Some(truncate_detail(&content, 120)),
        swarm.members,
        swarm.swarms_by_id,
        Some(swarm.event_history),
        Some(swarm.event_counter),
        Some(swarm.event_tx),
    )
    .await;

    let start_message_index = {
        let agent_guard = agent.lock().await;
        agent_guard.message_count()
    };
    let agent = Arc::clone(agent);
    let report_agent = Arc::clone(&agent);
    let tx = crate::server::state::session_event_fanout_sender_with_fallback(
        client_session_id.to_string(),
        Arc::clone(swarm.members),
        client_event_tx.clone(),
    );
    let done_tx = processing_done_tx.clone();
    let routing_members = Arc::clone(swarm.members);
    let routing_plans = Arc::clone(swarm.plans);
    let routing_session = client_session_id.to_string();
    crate::logging::info(&format!("Processing message id={} spawning task", id));
    *state.task = Some(tokio::spawn(async move {
        let event_tx = tx.clone();
        let result = match std::panic::AssertUnwindSafe(crate::hooks::with_client_terminal_env(
            client_terminal_env,
            process_routed_message_streaming_mpsc(
                agent,
                &content,
                images,
                system_reminder,
                event_tx,
                &routing_session,
                &routing_members,
                &routing_plans,
            ),
        ))
        .catch_unwind()
        .await
        {
            Ok(result) => result,
            Err(panic_payload) => {
                let msg = if let Some(text) = panic_payload.downcast_ref::<&str>() {
                    text.to_string()
                } else if let Some(text) = panic_payload.downcast_ref::<String>() {
                    text.clone()
                } else {
                    "unknown panic".to_string()
                };
                crate::logging::error(&format!(
                    "Processing task PANICKED for message id={}: {}",
                    id, msg
                ));
                Err(anyhow::anyhow!("Processing task panicked: {}", msg))
            }
        };
        match &result {
            Ok(()) => crate::logging::info(&format!(
                "Processing task completed OK for message id={}",
                id
            )),
            Err(error) => crate::logging::warn(&format!(
                "Processing task completed with error for message id={}: {}",
                id, error
            )),
        }
        let completion_report = if result.is_ok() {
            let agent = report_agent.lock().await;
            agent.latest_assistant_text_after(start_message_index)
        } else {
            None
        };
        // Keep the terminal event on the same ordered fanout channel as the
        // stream. Sending it later from the owning client's event loop could
        // race ahead of the final MessageEnd for newly attached clients.
        let terminal_event = match &result {
            Ok(()) => ServerEvent::Done { id },
            Err(error) => ServerEvent::Error {
                id,
                message: crate::util::format_error_chain(error),
                retry_after_secs: error
                    .downcast_ref::<StreamError>()
                    .and_then(|stream_error| stream_error.retry_after_secs),
            },
        };
        let _ = tx.send(terminal_event);
        let _ = done_tx.send((id, result, completion_report));
    }));
}

/// Process a message and stream events (mpsc channel - per-client)
#[expect(
    clippy::too_many_arguments,
    reason = "client dispatch needs current swarm plan and policy at consumption"
)]
pub(in crate::server) async fn process_routed_message_streaming_mpsc(
    agent: Arc<Mutex<Agent>>,
    content: &str,
    images: Vec<(String, String)>,
    system_reminder: Option<String>,
    event_tx: mpsc::UnboundedSender<ServerEvent>,
    session_id: &str,
    members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    plans: &Arc<RwLock<HashMap<String, VersionedPlan>>>,
) -> Result<()> {
    let mut agent = agent.lock().await;
    {
        // Only queued assignments constrain a new turn. Historical member
        // routing is not a reason to reject an ordinary user message.
        let plans = plans.read().await;
        let pending: Vec<_> = plans
            .values()
            .flat_map(|plan| {
                plan.items
                    .iter()
                    .filter(|item| {
                        item.status == "queued" && item.assigned_to.as_deref() == Some(session_id)
                    })
                    .map(|item| (&item.id, plan.node_meta.get(&item.id)))
            })
            .collect();
        if !pending.is_empty() {
            let members = members.try_read().map_err(|_| {
                anyhow::anyhow!("Task routing blocked: member policy is busy; retry when idle.")
            })?;
            for (item_id, meta) in pending {
                let routing = match meta {
                    Some(meta) => crate::protocol::SwarmRoutingStatus {
                        requested_model: meta.model.clone(),
                        requested_effort: meta.effort.clone(),
                        allowed_models: meta.allowed_models.clone(),
                        resolved_execution: meta.resolved_execution.clone(),
                        ..Default::default()
                    },
                    // Legacy queued items have no routing metadata, but still
                    // pass through current-policy validation below.
                    None => crate::protocol::SwarmRoutingStatus::default(),
                };
                let provider = agent.provider_handle();
                crate::server::comm_control::observe_node_execution(
                    session_id, session_id, &routing, routing.resolved_execution.is_some(),
                    &agent, &members, provider.as_ref(),
                ).map_err(|error| anyhow::anyhow!("Task '{}' routing blocked: {error}. Queued work retained; restore its route or reassign the task.", item_id))?;
            }
        }
    }
    // The same agent guard protects identity from validation into execution.
    // No global/member/plan lock is held during provider or tool streaming.
    process_locked_message_streaming_mpsc(&mut agent, content, images, system_reminder, event_tx)
        .await
}

/// Process a message and stream events (mpsc channel - per-client)
pub(in crate::server) async fn process_message_streaming_mpsc(
    agent: Arc<Mutex<Agent>>,
    content: &str,
    images: Vec<(String, String)>,
    system_reminder: Option<String>,
    event_tx: tokio::sync::mpsc::UnboundedSender<ServerEvent>,
) -> Result<()> {
    let mut agent = agent.lock().await;
    process_locked_message_streaming_mpsc(&mut agent, content, images, system_reminder, event_tx)
        .await
}

/// Same as [`process_message_streaming_mpsc`] for a caller that already holds
/// the agent lock (e.g. a wake turn that reserved the idle agent up front, see
/// #1152).
pub(in crate::server) async fn process_locked_message_streaming_mpsc(
    agent: &mut Agent,
    content: &str,
    images: Vec<(String, String)>,
    system_reminder: Option<String>,
    event_tx: tokio::sync::mpsc::UnboundedSender<ServerEvent>,
) -> Result<()> {
    let session_id = agent.session_id().to_string();
    let result = agent
        .run_once_streaming_mpsc(content, images, system_reminder, event_tx)
        .await;
    if result.is_ok() {
        crate::runtime_memory_log::emit_event(
            crate::runtime_memory_log::RuntimeMemoryLogEvent::new(
                "turn_completed",
                "message_turn_finished",
            )
            .with_session_id(session_id)
            .force_attribution(),
        );
        crate::process_memory::release_retained_heap_debounced(
            "server_turn_completed",
            std::time::Duration::from_secs(30),
        );
    }
    result
}
