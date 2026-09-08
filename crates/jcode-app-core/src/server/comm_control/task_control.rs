// Recovery and explicit control of already assigned tasks.
use super::*;

#[expect(
    clippy::too_many_arguments,
    reason = "task control checks assignment state, delivery, and safe recovery paths together"
)]
pub(in crate::server) async fn handle_comm_task_control(
    id: u64,
    req_session_id: String,
    action: String,
    task_id: String,
    target_session: Option<String>,
    message: Option<String>,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    sessions: &SessionAgents,
    provider_template: &Arc<dyn crate::provider::Provider>,
    soft_interrupt_queues: &crate::server::SessionInterruptQueues,
    client_connections: &Arc<RwLock<HashMap<String, ClientConnectionInfo>>>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    swarm_plans: &Arc<RwLock<HashMap<String, VersionedPlan>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
    swarm_mutation_runtime: &SwarmMutationRuntime,
) {
    let Some(action) = TaskControlAction::parse(&action) else {
        if let Err(error) = client_event_tx.send(ServerEvent::Error {
            id,
            message: "Unknown task control action. Use start, wake, resume, retry, reassign, replace, or salvage.".to_string(),
            retry_after_secs: None,
        }) {
crate::logging::debug(&format!("Could not deliver task execution event: {error}"));
}
        return;
    };

    let swarm_id = match require_plan_driver_swarm(
        id,
        &req_session_id,
        "Only the coordinator can control assigned tasks.",
        client_event_tx,
        swarm_members,
        swarm_plans,
        swarm_coordinators,
    )
    .await
    {
        Some(swarm_id) => swarm_id,
        None => return,
    };

    let task_id = if task_id.trim().is_empty() {
        let Some(target_session) = target_session.as_deref() else {
            if let Err(error) = client_event_tx.send(ServerEvent::Error {
                id,
                message: format!(
                    "task_id is required for {} unless target_session uniquely identifies an assigned task.",
                    action.as_str()
                ),
                retry_after_secs: None,
            }) {
crate::logging::debug(&format!("Could not deliver task execution event: {error}"));
}
            return;
        };
        match task_id_for_target_session(&swarm_id, target_session, action, swarm_plans).await {
            Ok(task_id) => task_id,
            Err(message) => {
                if let Err(error) = client_event_tx.send(ServerEvent::Error {
                    id,
                    message,
                    retry_after_secs: None,
                }) {
                    crate::logging::debug(&format!(
                        "Could not deliver task execution event: {error}"
                    ));
                }
                return;
            }
        }
    } else {
        task_id
    };

    let Some(snapshot) = task_snapshot_for(&swarm_id, &task_id, swarm_plans).await else {
        if let Err(error) = client_event_tx.send(ServerEvent::Error {
            id,
            message: format!("Task '{}' not found in swarm plan", task_id),
            retry_after_secs: None,
        }) {
            crate::logging::debug(&format!("Could not deliver task execution event: {error}"));
        }
        return;
    };

    if !task_control_action_allows_status(action, &snapshot.status) {
        if let Err(error) = client_event_tx.send(ServerEvent::Error {
            id,
            message: task_control_status_error(action, &snapshot.status, &task_id),
            retry_after_secs: None,
        }) {
            crate::logging::debug(&format!("Could not deliver task execution event: {error}"));
        }
        return;
    }

    let current_assignee = snapshot.assigned_to.clone();
    let require_assignee = matches!(
        action,
        TaskControlAction::Start
            | TaskControlAction::Wake
            | TaskControlAction::Resume
            | TaskControlAction::Retry
            | TaskControlAction::Replace
            | TaskControlAction::Salvage
            | TaskControlAction::Reassign
    );
    if require_assignee && current_assignee.is_none() {
        if let Err(error) = client_event_tx.send(ServerEvent::Error {
            id,
            message: format!(
                "Task '{}' is not currently assigned. Use assign_task to create the first assignment.",
                task_id
            ),
            retry_after_secs: None,
        }) {
crate::logging::debug(&format!("Could not deliver task execution event: {error}"));
}
        return;
    }

    match action {
        TaskControlAction::Start | TaskControlAction::Wake | TaskControlAction::Resume => {
            let Some(assignee) = current_assignee.clone() else {
                if let Err(error) = client_event_tx.send(ServerEvent::Error {
                    id,
                    message: format!(
                        "Task '{}' no longer has an assignee. Use assign_task to create the first assignment.",
                        task_id
                    ),
                    retry_after_secs: None,
                }) {
crate::logging::debug(&format!("Could not deliver task execution event: {error}"));
}
                return;
            };
            if let Some(ref requested_target) = target_session
                && requested_target != &assignee
            {
                if let Err(error) = client_event_tx.send(ServerEvent::Error {
                    id,
                    message: format!(
                        "Task '{}' is assigned to '{}', not '{}'. Use reassign or replace to change ownership.",
                        task_id, assignee, requested_target
                    ),
                    retry_after_secs: None,
                }) {
crate::logging::debug(&format!("Could not deliver task execution event: {error}"));
}
                return;
            }

            // Legacy plans omit node metadata and retain inherited routing.
            let admitted_meta = swarm_plans
                .read()
                .await
                .get(&swarm_id)
                .and_then(|plan| plan.node_meta.get(&task_id))
                .cloned()
                .unwrap_or_else(NodeMeta::default);
            let mut routing = match admit_node_execution(
                &req_session_id,
                &admitted_meta,
                None,
                None,
                None,
                sessions,
                swarm_members,
                provider_template,
            )
            .await
            {
                Ok(routing) => routing,
                Err(error) => {
                    if let Err(error) = client_event_tx.send(ServerEvent::Error {
                        id,
                        message: format!("Task '{task_id}' routing blocked: {error}"),
                        retry_after_secs: None,
                    }) {
                        crate::logging::debug(&format!(
                            "Could not deliver task execution event: {error}"
                        ));
                    }
                    return;
                }
            };
            let assignment_text =
                build_control_assignment_text(action, &snapshot.content, message.as_deref());
            let result = {
                let mut plans = swarm_plans.write().await;
                (|| -> anyhow::Result<()> {
                    let agents = sessions
                        .try_read()
                        .map_err(|_| anyhow::anyhow!("Session map is busy; retry control."))?;
                    let agent = agents
                        .get(&assignee)
                        .cloned()
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Assigned session is unavailable; replace or salvage the task."
                            )
                        })?
                        .try_lock_owned()
                        .map_err(|_| {
                            anyhow::anyhow!(
                                "Assigned session is currently busy; retry control when idle."
                            )
                        })?;
                    let mut members = swarm_members
                        .try_write()
                        .map_err(|_| anyhow::anyhow!("Member policy is busy; retry control."))?;
                    routing = observe_node_execution(
                        &req_session_id,
                        &assignee,
                        &routing,
                        admitted_meta.resolved_execution.is_some(),
                        &agent,
                        &members,
                        provider_template.as_ref(),
                    )?;
                    let plan = plans
                        .get_mut(&swarm_id)
                        .ok_or_else(|| anyhow::anyhow!("No swarm plan exists."))?;
                    if plan.node_meta.get(&task_id).unwrap_or(&NodeMeta::default())
                        != &admitted_meta
                        || !plan.items.iter().any(|item| {
                            item.id == task_id
                                && item.status == snapshot.status
                                && item.assigned_to == snapshot.assigned_to
                        })
                    {
                        anyhow::bail!("Node changed during admission; retry control.");
                    }
                    let mut staged = plan.clone();
                    if snapshot.status != "queued" {
                        requeue_assignment_in_plan(
                            &mut staged,
                            &req_session_id,
                            &assignee,
                            &task_id,
                            &assignment_text,
                        )
                        .ok_or_else(|| anyhow::anyhow!("Task disappeared during admission."))?;
                    }
                    pin_node_execution(
                        staged.node_meta.entry(task_id.clone()).or_default(),
                        &routing,
                    );
                    routing.resolved_execution =
                        staged.node_meta[&task_id].resolved_execution.clone();
                    *plan = staged;
                    if let Some(member) = members.get_mut(&assignee) {
                        member.allowed_models = routing.allowed_models.clone();
                        member.runtime.routing = Some(routing.clone());
                    }
                    spawn_assigned_task_run(
                        agent,
                        assignee.clone(),
                        swarm_id.clone(),
                        task_id.clone(),
                        assignment_text,
                        Arc::clone(swarm_members),
                        Arc::clone(swarms_by_id),
                        Arc::clone(swarm_plans),
                        Arc::clone(swarm_coordinators),
                        Arc::clone(event_history),
                        Arc::clone(event_counter),
                        swarm_event_tx.clone(),
                    );
                    Ok(())
                })()
            };
            if let Err(error) = result {
                if let Err(error) = client_event_tx.send(ServerEvent::Error {
                    id,
                    message: format!("Task '{task_id}' routing blocked: {error}"),
                    retry_after_secs: Some(1),
                }) {
                    crate::logging::debug(&format!(
                        "Could not deliver task execution event: {error}"
                    ));
                }
                return;
            }
            let swarm_state = SwarmState {
                members: Arc::clone(swarm_members),
                swarms_by_id: Arc::clone(swarms_by_id),
                plans: Arc::clone(swarm_plans),
                coordinators: Arc::clone(swarm_coordinators),
            };
            persist_swarm_state_for(&swarm_id, &swarm_state).await;
            broadcast_swarm_plan(
                &swarm_id,
                Some(format!("task_{}", action.as_str())),
                swarm_plans,
                swarm_members,
                swarms_by_id,
            )
            .await;
            let summary = plan_graph_status_for(&swarm_id, swarm_plans).await;
            if let Err(error) = client_event_tx.send(ServerEvent::CommTaskControlResponse {
                id,
                action: action.as_str().to_string(),
                task_id: task_id.clone(),
                target_session: Some(assignee),
                status: "running".to_string(),
                summary,
                routing: Some(routing),
            }) {
                crate::logging::debug(&format!("Could not deliver task execution event: {error}"));
            }
        }
        TaskControlAction::Retry => {
            let Some(assignee) = current_assignee.clone() else {
                if let Err(error) = client_event_tx.send(ServerEvent::Error {
                    id,
                    message: format!(
                        "Task '{}' no longer has an assignee. Use assign_task to create the first assignment.",
                        task_id
                    ),
                    retry_after_secs: None,
                }) {
crate::logging::debug(&format!("Could not deliver task execution event: {error}"));
}
                return;
            };
            let retry_note = message.as_ref().map_or_else(
                || "Retry this assignment.".to_string(),
                |extra| {
                    format!(
                        "Retry this assignment.\n\nAdditional coordinator instructions:\n{}",
                        extra
                    )
                },
            );
            handle_comm_assign_task_with_mode(
                id,
                req_session_id,
                Some(assignee),
                Some(task_id),
                None,
                None,
                Some(retry_note),
                AssignDedupMode::AlwaysDispatch,
                None,
                client_event_tx,
                sessions,
                provider_template,
                soft_interrupt_queues,
                client_connections,
                swarm_members,
                swarms_by_id,
                swarm_plans,
                swarm_coordinators,
                event_history,
                event_counter,
                swarm_event_tx,
                swarm_mutation_runtime,
            )
            .await;
        }
        TaskControlAction::Reassign | TaskControlAction::Replace | TaskControlAction::Salvage => {
            let Some(assignee) = current_assignee.clone() else {
                if let Err(error) = client_event_tx.send(ServerEvent::Error {
                    id,
                    message: format!(
                        "Task '{}' no longer has an assignee. Use assign_task to create the first assignment.",
                        task_id
                    ),
                    retry_after_secs: None,
                }) {
crate::logging::debug(&format!("Could not deliver task execution event: {error}"));
}
                return;
            };
            let Some(new_target) = target_session else {
                if let Err(error) = client_event_tx.send(ServerEvent::Error {
                    id,
                    message: format!("'target_session' is required for {}.", action.as_str()),
                    retry_after_secs: None,
                }) {
                    crate::logging::debug(&format!(
                        "Could not deliver task execution event: {error}"
                    ));
                }
                return;
            };

            if new_target == assignee {
                if let Err(error) = client_event_tx.send(ServerEvent::Error {
                    id,
                    message: format!("Task '{}' is already assigned to '{}'.", task_id, assignee),
                    retry_after_secs: None,
                }) {
                    crate::logging::debug(&format!(
                        "Could not deliver task execution event: {error}"
                    ));
                }
                return;
            }

            if snapshot.status == "running" {
                if let Err(error) = client_event_tx.send(ServerEvent::Error {
                    id,
                    message: format!(
                        "Task '{}' is actively running on '{}'. Wait, wake, or stop that agent before handing the task off.",
                        task_id, assignee
                    ),
                    retry_after_secs: Some(1),
                }) {
crate::logging::debug(&format!("Could not deliver task execution event: {error}"));
}
                return;
            }

            if action == TaskControlAction::Replace
                && !matches!(
                    snapshot.status.as_str(),
                    "queued" | "failed" | "stopped" | "crashed" | "running_stale"
                )
            {
                if let Err(error) = client_event_tx.send(ServerEvent::Error {
                    id,
                    message: format!(
                        "Task '{}' is '{}' and cannot be safely replaced.",
                        task_id, snapshot.status
                    ),
                    retry_after_secs: None,
                }) {
                    crate::logging::debug(&format!(
                        "Could not deliver task execution event: {error}"
                    ));
                }
                return;
            }

            let forwarded_message = if action == TaskControlAction::Salvage {
                let prior_name = active_swarm_member(&assignee, swarm_members)
                    .await
                    .and_then(|member| member.friendly_name);
                let summaries =
                    if let Some(agent_arc) = task_agent_session(&assignee, sessions).await {
                        if let Ok(agent) = agent_arc.try_lock() {
                            agent.get_tool_call_summaries(12)
                        } else {
                            vec![]
                        }
                    } else {
                        vec![]
                    };
                let mut salvage = format_salvage_message(
                    &assignee,
                    prior_name.as_deref(),
                    &summaries,
                    message.as_deref(),
                );
                if let Some(progress) = snapshot.progress.as_ref() {
                    if let Some(summary) = progress.checkpoint_summary.as_deref() {
                        salvage.push_str("\n\nLatest checkpoint summary:\n");
                        salvage.push_str(summary);
                    }
                    if let Some(detail) = progress.last_detail.as_deref() {
                        salvage.push_str("\n\nLatest recorded detail:\n");
                        salvage.push_str(detail);
                    }
                }
                Some(salvage)
            } else if action == TaskControlAction::Replace {
                Some(message.as_ref().map_or_else(
                    || format!("This task is replacing prior assignee '{}'.", assignee),
                    |extra| format!(
                        "This task is replacing prior assignee '{}'.\n\nAdditional coordinator instructions:\n{}",
                        assignee, extra
                    ),
                ))
            } else {
                message
            };

            let displaced_task_id = task_id.clone();
            let displaced_new_target = new_target.clone();
            let displaced_req_session = req_session_id.clone();
            handle_comm_assign_task_with_mode(
                id,
                req_session_id,
                Some(new_target),
                Some(task_id),
                None,
                None,
                forwarded_message,
                AssignDedupMode::AlwaysDispatch,
                None,
                client_event_tx,
                sessions,
                provider_template,
                soft_interrupt_queues,
                client_connections,
                swarm_members,
                swarms_by_id,
                swarm_plans,
                swarm_coordinators,
                event_history,
                event_counter,
                swarm_event_tx,
                swarm_mutation_runtime,
            )
            .await;

            // Tell the displaced worker to stand down, but only when the
            // takeover actually landed (the re-dispatch above can still fail,
            // e.g. on an unknown target session, and then the prior assignee
            // keeps the task). Without this the displaced worker keeps
            // editing the same files as its replacement until a human DMs it.
            let takeover_landed = {
                let swarm_plans = swarm_plans.read().await;
                swarm_plans
                    .get(&swarm_id)
                    .and_then(|plan| plan.items.iter().find(|item| item.id == displaced_task_id))
                    .is_some_and(|item| {
                        item.assigned_to.as_deref() == Some(displaced_new_target.as_str())
                    })
            };
            if takeover_landed {
                let stand_down = format!(
                    "Task '{}' has been handed off to '{}' by the coordinator ({}). Stop working \
                     on it immediately: do not make further edits or commits for that task. If \
                     you have uncommitted progress worth keeping, note it in a brief message to \
                     the coordinator, then stand down.",
                    displaced_task_id,
                    displaced_new_target,
                    action.as_str()
                );
                if !queue_soft_interrupt_for_session(
                    &assignee,
                    stand_down.clone(),
                    true,
                    SoftInterruptSource::System,
                    soft_interrupt_queues,
                    sessions,
                )
                .await
                {
                    crate::logging::warn(&format!(
                        "Task '{displaced_task_id}' takeover committed, but displaced worker '{assignee}' could not receive the stand-down interrupt"
                    ));
                }
                if let Some(member) = swarm_members.read().await.get(&assignee) {
                    if let Err(error) = member.event_tx.send(ServerEvent::Notification {
                        from_session: displaced_req_session,
                        from_name: None,
                        notification_type: NotificationType::Message {
                            scope: Some("dm".to_string()),
                            channel: None,
                            tldr: None,
                        },
                        message: stand_down,
                    }) {
                        crate::logging::debug(&format!(
                            "Could not deliver task execution event: {error}"
                        ));
                    }
                }
            }
        }
    }
}
