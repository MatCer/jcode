use super::swarm_mutation_state::{
    PersistedSwarmMutationResponse, SwarmMutationRuntime, begin_or_replay, finish_request,
    request_key,
};
use super::{
    SessionInterruptQueues, SharedContext, SwarmEvent, SwarmEventType, SwarmMember, SwarmState,
    VersionedPlan, broadcast_swarm_plan, persist_swarm_state_for, queue_soft_interrupt_for_session,
    record_swarm_event, summarize_plan_items,
};
use crate::agent::Agent;
use crate::plan::PlanItem;
use crate::protocol::{NotificationType, ServerEvent};
use jcode_agent_runtime::SoftInterruptSource;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc};

type SessionAgents = Arc<RwLock<HashMap<String, Arc<Mutex<Agent>>>>>;

/// Reject plans whose dependency graph contains a cycle. Cyclic items can never
/// become runnable (`summarize_plan_graph` parks them in `blocked_ids` forever),
/// so letting one into a live plan silently wedges all dependent work. This
/// mirrors the acyclicity validation `jcode_plan::dag::seed`/`expand` already
/// enforce on the task-graph paths. Returns a user-facing error naming the
/// cyclic item ids, or `None` when the graph is a valid DAG.
fn plan_cycle_error(items: &[PlanItem]) -> Option<String> {
    let cycle_ids = crate::plan::cycle_item_ids(items);
    if cycle_ids.is_empty() {
        return None;
    }
    Some(format!(
        "Plan rejected: dependency cycle among item(s): {}. Fix the blocked_by \
         edges so the plan forms a DAG, then propose again.",
        cycle_ids.join(", ")
    ))
}

async fn admit_legacy_plan_items(
    requester: &str,
    items: &[PlanItem],
    plan: &VersionedPlan,
    inherited: Option<Vec<String>>,
    sessions: &SessionAgents,
    members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    provider: &Arc<dyn crate::provider::Provider>,
) -> anyhow::Result<HashMap<String, jcode_plan::NodeMeta>> {
    let mut metadata = HashMap::new();
    for item in items {
        if plan.items.iter().any(|existing| existing.id == item.id) {
            continue;
        }
        let mut meta = jcode_plan::NodeMeta {
            allowed_models: inherited.clone(),
            ..Default::default()
        };
        let routing = super::comm_control::admit_node_execution(
            requester, &meta, None, None, None, sessions, members, provider,
        )
        .await?;
        meta.allowed_models = routing.allowed_models;
        metadata.insert(item.id.clone(), meta);
    }
    Ok(metadata)
}

#[expect(
    clippy::too_many_arguments,
    reason = "plan proposal updates sessions, swarm coordination, shared context, interrupts, and event history"
)]
pub(super) async fn handle_comm_propose_plan(
    id: u64,
    req_session_id: String,
    items: Vec<PlanItem>,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    shared_context: &Arc<RwLock<HashMap<String, HashMap<String, SharedContext>>>>,
    swarm_plans: &Arc<RwLock<HashMap<String, VersionedPlan>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
    sessions: &SessionAgents,
    soft_interrupt_queues: &SessionInterruptQueues,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
    _swarm_mutation_runtime: &SwarmMutationRuntime,
    provider_template: &Arc<dyn crate::provider::Provider>,
) {
    let swarm_id = {
        let members = swarm_members.read().await;
        members
            .get(&req_session_id)
            .and_then(|member| member.swarm_id.clone())
    };

    let swarm_id = match swarm_id.as_ref() {
        Some(swarm_id) => swarm_id.clone(),
        None => {
            let _ = client_event_tx.send(ServerEvent::Error {
                id,
                message: "Not in a swarm.".to_string(),
                retry_after_secs: None,
            });
            return;
        }
    };

    let (from_name, coordinator_id) = {
        let members = swarm_members.read().await;
        let from_name = members
            .get(&req_session_id)
            .and_then(|member| member.friendly_name.clone());
        let coordinators = swarm_coordinators.read().await;
        let coordinator_id = coordinators.get(&swarm_id).cloned();
        (from_name, coordinator_id)
    };
    let from_label = from_name
        .clone()
        .unwrap_or_else(|| req_session_id.chars().take(8).collect());

    let Some(coordinator_id) = coordinator_id else {
        let _ = client_event_tx.send(ServerEvent::Error {
            id,
            message: "No coordinator for this swarm.".to_string(),
            retry_after_secs: None,
        });
        return;
    };

    if coordinator_id == req_session_id {
        // Close the last entry path for dependency cycles into live plans: the
        // dag seed/expand ops validate acyclicity, but this direct-update path
        // used to write `plan.items` verbatim. A cyclic task can never become
        // runnable (it lands in blocked_ids forever), silently wedging all
        // dependent work, so reject for light plans too, matching `dag::seed`
        // which rejects cycles in both modes.
        if let Some(message) = plan_cycle_error(&items) {
            let _ = client_event_tx.send(ServerEvent::Error {
                id,
                message,
                retry_after_secs: None,
            });
            return;
        }
        let admitted_plan = swarm_plans
            .read()
            .await
            .get(&swarm_id)
            .cloned()
            .unwrap_or_default();
        let metadata = match admit_legacy_plan_items(
            &req_session_id,
            &items,
            &admitted_plan,
            None,
            sessions,
            swarm_members,
            provider_template,
        )
        .await
        {
            Ok(metadata) => metadata,
            Err(error) => {
                if let Err(error) = client_event_tx.send(ServerEvent::Error {
                    id,
                    message: format!("Plan routing rejected: {error}"),
                    retry_after_secs: None,
                }) {
                    crate::logging::debug(&format!("Could not deliver swarm plan event: {error}"));
                }
                return;
            }
        };
        let (version, participant_ids) = {
            let mut plans = swarm_plans.write().await;
            let plan = plans
                .entry(swarm_id.clone())
                .or_insert_with(VersionedPlan::new);
            if plan.version != admitted_plan.version {
                if let Err(error) = client_event_tx.send(ServerEvent::Error {
                    id,
                    message: "Plan changed during admission; retry update.".into(),
                    retry_after_secs: None,
                }) {
                    crate::logging::debug(&format!("Could not deliver swarm plan event: {error}"));
                }
                return;
            }
            plan.node_meta.extend(metadata);
            plan.participants.insert(req_session_id.clone());
            for item in &items {
                if let Some(owner) = &item.assigned_to {
                    plan.participants.insert(owner.clone());
                }
            }
            plan.replace_items(items.clone());
            plan.version += 1;
            (plan.version, plan.participants.clone())
        };

        let members = swarm_members.read().await;
        let notification_msg = format!(
            "Plan updated by {} ({} items, v{})",
            from_label,
            items.len(),
            version
        );
        for sid in participant_ids {
            if sid == req_session_id {
                continue;
            }
            if let Some(member) = members.get(&sid) {
                let _ = member.event_tx.send(ServerEvent::Notification {
                    from_session: req_session_id.clone(),
                    from_name: from_name.clone(),
                    notification_type: NotificationType::Message {
                        scope: Some("plan".to_string()),
                        channel: None,
                        tldr: None,
                    },
                    message: notification_msg.clone(),
                });
            }
            let _ = queue_soft_interrupt_for_session(
                &sid,
                notification_msg.clone(),
                false,
                SoftInterruptSource::System,
                soft_interrupt_queues,
                sessions,
            )
            .await;
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
            Some("coordinator_direct_update".to_string()),
            swarm_plans,
            swarm_members,
            swarms_by_id,
        )
        .await;
        record_swarm_event(
            event_history,
            event_counter,
            swarm_event_tx,
            req_session_id.clone(),
            from_name.clone(),
            Some(swarm_id.clone()),
            SwarmEventType::PlanUpdate {
                swarm_id: swarm_id.clone(),
                item_count: items.len(),
            },
        )
        .await;

        if let Err(error) = client_event_tx.send(ServerEvent::Done { id }) {
            crate::logging::debug(&format!("Could not deliver swarm plan event: {error}"));
        }
        return;
    }

    // Non-coordinator proposal path. A cycle among the proposed items alone is
    // a cycle in any merged graph, so reject it here for immediate proposer
    // feedback instead of storing a proposal that can only bounce at approval.
    // (Cycles that only form against the *existing* plan are caught at
    // approve-time, where the merge is authoritative.)
    if let Some(message) = plan_cycle_error(&items) {
        let _ = client_event_tx.send(ServerEvent::Error {
            id,
            message,
            retry_after_secs: None,
        });
        return;
    }

    let proposal_key = format!("plan_proposal:{req_session_id}");
    let proposal_value = serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string());
    {
        let mut context = shared_context.write().await;
        let swarm_context = context.entry(swarm_id.clone()).or_insert_with(HashMap::new);
        let now = Instant::now();
        swarm_context.insert(
            proposal_key.clone(),
            SharedContext {
                key: proposal_key.clone(),
                value: proposal_value,
                from_session: req_session_id.clone(),
                from_name: from_name.clone(),
                created_at: now,
                updated_at: now,
            },
        );
    }
    record_swarm_event(
        event_history,
        event_counter,
        swarm_event_tx,
        req_session_id.clone(),
        from_name.clone(),
        Some(swarm_id.clone()),
        SwarmEventType::PlanProposal {
            swarm_id: swarm_id.clone(),
            proposer_session: req_session_id.clone(),
            item_count: items.len(),
        },
    )
    .await;

    let summary = summarize_plan_items(&items, 3);
    let notification_msg = format!(
        "Plan proposal from {} ({} items). Summary: {}. Review with communicate read key '{}'.",
        from_label,
        items.len(),
        summary,
        proposal_key
    );

    let members = swarm_members.read().await;
    if let Some(member) = members.get(&coordinator_id) {
        let _ = member.event_tx.send(ServerEvent::Notification {
            from_session: req_session_id.clone(),
            from_name: from_name.clone(),
            notification_type: NotificationType::Message {
                scope: Some("plan_proposal".to_string()),
                channel: None,
                tldr: None,
            },
            message: notification_msg.clone(),
        });
        let _ = member.event_tx.send(ServerEvent::SwarmPlanProposal {
            swarm_id: swarm_id.clone(),
            proposer_session: req_session_id.clone(),
            proposer_name: from_name.clone(),
            items: items.clone(),
            summary: summary.clone(),
            proposal_key: proposal_key.clone(),
        });
    }
    let _ = queue_soft_interrupt_for_session(
        &coordinator_id,
        notification_msg.clone(),
        false,
        SoftInterruptSource::System,
        soft_interrupt_queues,
        sessions,
    )
    .await;

    let proposer_confirmation = "Plan proposal sent to coordinator (not yet applied).".to_string();
    if let Some(member) = members.get(&req_session_id) {
        let _ = member.event_tx.send(ServerEvent::Notification {
            from_session: req_session_id.clone(),
            from_name: from_name.clone(),
            notification_type: NotificationType::Message {
                scope: Some("plan_proposal".to_string()),
                channel: None,
                tldr: None,
            },
            message: proposer_confirmation.clone(),
        });
    }
    let _ = queue_soft_interrupt_for_session(
        &req_session_id,
        proposer_confirmation,
        false,
        SoftInterruptSource::System,
        soft_interrupt_queues,
        sessions,
    )
    .await;

    if let Err(error) = client_event_tx.send(ServerEvent::Done { id }) {
        crate::logging::debug(&format!("Could not deliver swarm plan event: {error}"));
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "plan approval updates sessions, swarm coordination, interrupts, and event history"
)]
pub(super) async fn handle_comm_approve_plan(
    id: u64,
    req_session_id: String,
    proposer_session: String,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    shared_context: &Arc<RwLock<HashMap<String, HashMap<String, SharedContext>>>>,
    swarm_plans: &Arc<RwLock<HashMap<String, VersionedPlan>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
    sessions: &SessionAgents,
    soft_interrupt_queues: &SessionInterruptQueues,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
    swarm_mutation_runtime: &SwarmMutationRuntime,
    provider_template: &Arc<dyn crate::provider::Provider>,
) {
    let swarm_id = match require_coordinator_swarm(
        id,
        &req_session_id,
        "Only the coordinator can approve plan proposals.",
        client_event_tx,
        swarm_members,
        swarm_coordinators,
    )
    .await
    {
        Some(swarm_id) => swarm_id,
        None => return,
    };

    let mutation_key = request_key(
        &req_session_id,
        "approve_plan",
        &[swarm_id.clone(), proposer_session.clone()],
    );
    let Some(mutation_state) = begin_or_replay(
        swarm_mutation_runtime,
        &mutation_key,
        "approve_plan",
        &req_session_id,
        id,
        client_event_tx,
    )
    .await
    else {
        return;
    };

    let proposal_key = format!("plan_proposal:{proposer_session}");
    let proposal_value = {
        let context = shared_context.read().await;
        context
            .get(&swarm_id)
            .and_then(|swarm_context| swarm_context.get(&proposal_key))
            .map(|context| (context.value.clone(), context.from_session.clone()))
    };

    let (proposal, proposal_author) = match proposal_value {
        Some(proposal) => proposal,
        None => {
            finish_request(
                swarm_mutation_runtime,
                &mutation_state,
                PersistedSwarmMutationResponse::Error {
                    message: format!("No pending plan proposal from session '{proposer_session}'"),
                    retry_after_secs: None,
                },
            )
            .await;
            return;
        }
    };

    let author_allowed = if proposal_author != proposer_session {
        Err(anyhow::anyhow!(
            "Proposal provenance does not match its author; ask the author to re-propose."
        ))
    } else {
        let members = swarm_members.read().await;
        if members
            .get(&proposer_session)
            .is_none_or(|member| member.swarm_id.as_deref() != Some(&swarm_id))
        {
            Err(anyhow::anyhow!(
                "Proposal author is unavailable; ask a live member to re-propose."
            ))
        } else {
            super::comm_session::effective_swarm_allowed_models(
                &members,
                &proposer_session,
                None,
                None,
                &provider_template.model_routes(),
            )
        }
    };
    let author_allowed = match author_allowed {
        Ok(allowed) => allowed,
        Err(error) => {
            finish_request(swarm_mutation_runtime, &mutation_state, PersistedSwarmMutationResponse::Error {
                message: format!("Proposal routing provenance rejected: {error}. Re-propose from a live trusted session."), retry_after_secs: None,
            }).await;
            return;
        }
    };
    if let Ok(items) = serde_json::from_str::<Vec<PlanItem>>(&proposal) {
        let existing_count = swarm_plans
            .read()
            .await
            .get(&swarm_id)
            .map(|plan| plan.items.len())
            .unwrap_or_default();
        let merged_count = existing_count.saturating_add(items.len());
        if merged_count > jcode_plan::MAX_PLAN_ITEMS {
            finish_request(
                swarm_mutation_runtime,
                &mutation_state,
                PersistedSwarmMutationResponse::Error {
                    message: format!(
                        "Plan approval would contain {merged_count} items, exceeding the per-swarm limit of {}; finish or clear stale plan nodes first.",
                        jcode_plan::MAX_PLAN_ITEMS
                    ),
                    retry_after_secs: None,
                },
            )
            .await;
            return;
        }
        // Validate the merged graph (existing plan + proposed items) for
        // dependency cycles before committing. A cycle here permanently wedges
        // every task that depends on it, so reject the approval and keep the
        // proposal pending for the proposer to fix and re-propose.
        let merged_cycle_error = {
            let plans = swarm_plans.read().await;
            let mut merged: Vec<PlanItem> = plans
                .get(&swarm_id)
                .map(|plan| plan.items.clone())
                .unwrap_or_default();
            merged.extend(items.iter().cloned());
            plan_cycle_error(&merged)
        };
        if let Some(message) = merged_cycle_error {
            finish_request(
                swarm_mutation_runtime,
                &mutation_state,
                PersistedSwarmMutationResponse::Error {
                    message,
                    retry_after_secs: None,
                },
            )
            .await;
            return;
        }

        let admitted_plan = swarm_plans
            .read()
            .await
            .get(&swarm_id)
            .cloned()
            .unwrap_or_default();
        let metadata = match admit_legacy_plan_items(
            &req_session_id,
            &items,
            &admitted_plan,
            author_allowed,
            sessions,
            swarm_members,
            provider_template,
        )
        .await
        {
            Ok(metadata) => metadata,
            Err(error) => {
                finish_request(
                    swarm_mutation_runtime,
                    &mutation_state,
                    PersistedSwarmMutationResponse::Error {
                        message: format!("Proposal routing rejected: {error}"),
                        retry_after_secs: None,
                    },
                )
                .await;
                return;
            }
        };
        let participant_ids = {
            let mut plans = swarm_plans.write().await;
            let plan = plans
                .entry(swarm_id.clone())
                .or_insert_with(VersionedPlan::new);
            if plan.version != admitted_plan.version {
                drop(plans);
                finish_request(
                    swarm_mutation_runtime,
                    &mutation_state,
                    PersistedSwarmMutationResponse::Error {
                        message: "Plan changed during admission; retry approval.".into(),
                        retry_after_secs: None,
                    },
                )
                .await;
                return;
            }
            plan.node_meta.extend(metadata);
            plan.items.extend(items.clone());
            plan.version += 1;
            plan.participants.insert(req_session_id.clone());
            plan.participants.insert(proposer_session.clone());
            for item in &items {
                if let Some(owner) = &item.assigned_to {
                    plan.participants.insert(owner.clone());
                }
            }
            plan.participants.clone()
        };

        {
            let mut context = shared_context.write().await;
            if let Some(swarm_context) = context.get_mut(&swarm_id) {
                swarm_context.remove(&proposal_key);
            }
        }

        broadcast_swarm_plan(
            &swarm_id,
            Some("proposal_approved".to_string()),
            swarm_plans,
            swarm_members,
            swarms_by_id,
        )
        .await;
        record_swarm_event(
            event_history,
            event_counter,
            swarm_event_tx,
            req_session_id.clone(),
            None,
            Some(swarm_id.clone()),
            SwarmEventType::PlanUpdate {
                swarm_id: swarm_id.clone(),
                item_count: items.len(),
            },
        )
        .await;

        let coordinator_name = {
            let members = swarm_members.read().await;
            members
                .get(&req_session_id)
                .and_then(|member| member.friendly_name.clone())
        };

        let members = swarm_members.read().await;
        for sid in participant_ids {
            if let Some(member) = members.get(&sid) {
                let message = format!(
                    "Plan approved by coordinator: {} items added from {}",
                    items.len(),
                    proposer_session
                );
                let _ = member.event_tx.send(ServerEvent::Notification {
                    from_session: req_session_id.clone(),
                    from_name: coordinator_name.clone(),
                    notification_type: NotificationType::Message {
                        scope: Some("plan".to_string()),
                        channel: None,
                        tldr: None,
                    },
                    message: message.clone(),
                });

                let _ = queue_soft_interrupt_for_session(
                    &sid,
                    message.clone(),
                    false,
                    SoftInterruptSource::System,
                    soft_interrupt_queues,
                    sessions,
                )
                .await;
            }
        }

        let swarm_state = SwarmState {
            members: Arc::clone(swarm_members),
            swarms_by_id: Arc::clone(swarms_by_id),
            plans: Arc::clone(swarm_plans),
            coordinators: Arc::clone(swarm_coordinators),
        };
        persist_swarm_state_for(&swarm_id, &swarm_state).await;
    }

    finish_request(
        swarm_mutation_runtime,
        &mutation_state,
        PersistedSwarmMutationResponse::Done,
    )
    .await;
}

#[expect(
    clippy::too_many_arguments,
    reason = "plan rejection updates sessions, swarm coordination, interrupts, and event history"
)]
pub(super) async fn handle_comm_reject_plan(
    id: u64,
    req_session_id: String,
    proposer_session: String,
    reason: Option<String>,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    shared_context: &Arc<RwLock<HashMap<String, HashMap<String, SharedContext>>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
    sessions: &SessionAgents,
    soft_interrupt_queues: &SessionInterruptQueues,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
    swarm_mutation_runtime: &SwarmMutationRuntime,
) {
    let swarm_id = match require_coordinator_swarm(
        id,
        &req_session_id,
        "Only the coordinator can reject plan proposals.",
        client_event_tx,
        swarm_members,
        swarm_coordinators,
    )
    .await
    {
        Some(swarm_id) => swarm_id,
        None => return,
    };

    let mutation_key = request_key(
        &req_session_id,
        "reject_plan",
        &[
            swarm_id.clone(),
            proposer_session.clone(),
            reason.clone().unwrap_or_default(),
        ],
    );
    let Some(mutation_state) = begin_or_replay(
        swarm_mutation_runtime,
        &mutation_key,
        "reject_plan",
        &req_session_id,
        id,
        client_event_tx,
    )
    .await
    else {
        return;
    };

    let proposal_key = format!("plan_proposal:{proposer_session}");
    let proposal_exists = {
        let context = shared_context.read().await;
        context
            .get(&swarm_id)
            .and_then(|swarm_context| swarm_context.get(&proposal_key))
            .is_some()
    };

    if !proposal_exists {
        finish_request(
            swarm_mutation_runtime,
            &mutation_state,
            PersistedSwarmMutationResponse::Error {
                message: format!("No pending plan proposal from session '{proposer_session}'"),
                retry_after_secs: None,
            },
        )
        .await;
        return;
    }

    {
        let mut context = shared_context.write().await;
        if let Some(swarm_context) = context.get_mut(&swarm_id) {
            swarm_context.remove(&proposal_key);
        }
    }

    let coordinator_name = {
        let members = swarm_members.read().await;
        members
            .get(&req_session_id)
            .and_then(|member| member.friendly_name.clone())
    };

    let members = swarm_members.read().await;
    if let Some(member) = members.get(&proposer_session) {
        let reason_msg = reason
            .as_ref()
            .map(|reason| format!(": {reason}"))
            .unwrap_or_default();
        let message = format!("Your plan proposal was rejected by the coordinator{reason_msg}");
        let _ = member.event_tx.send(ServerEvent::Notification {
            from_session: req_session_id.clone(),
            from_name: coordinator_name.clone(),
            notification_type: NotificationType::Message {
                scope: Some("dm".to_string()),
                channel: None,
                tldr: None,
            },
            message: message.clone(),
        });

        let _ = queue_soft_interrupt_for_session(
            &proposer_session,
            message,
            false,
            SoftInterruptSource::System,
            soft_interrupt_queues,
            sessions,
        )
        .await;
    }
    record_swarm_event(
        event_history,
        event_counter,
        swarm_event_tx,
        req_session_id.clone(),
        coordinator_name,
        Some(swarm_id.clone()),
        SwarmEventType::Notification {
            notification_type: "plan_rejected".to_string(),
            message: proposer_session.clone(),
        },
    )
    .await;

    finish_request(
        swarm_mutation_runtime,
        &mutation_state,
        PersistedSwarmMutationResponse::Done,
    )
    .await;
}

#[cfg(test)]
#[path = "comm_plan_tests.rs"]
mod tests;

async fn require_coordinator_swarm(
    id: u64,
    req_session_id: &str,
    permission_error: &str,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
) -> Option<String> {
    let (swarm_id, is_coordinator) = {
        let members = swarm_members.read().await;
        let swarm_id = members
            .get(req_session_id)
            .and_then(|member| member.swarm_id.clone());
        let is_coordinator = if let Some(ref swarm_id) = swarm_id {
            let coordinators = swarm_coordinators.read().await;
            coordinators
                .get(swarm_id)
                .map(|coordinator| coordinator == req_session_id)
                .unwrap_or(false)
        } else {
            false
        };
        (swarm_id, is_coordinator)
    };

    if !is_coordinator {
        let _ = client_event_tx.send(ServerEvent::Error {
            id,
            message: permission_error.to_string(),
            retry_after_secs: None,
        });
        return None;
    }

    match swarm_id {
        Some(swarm_id) => Some(swarm_id),
        None => {
            let _ = client_event_tx.send(ServerEvent::Error {
                id,
                message: "Not in a swarm.".to_string(),
                retry_after_secs: None,
            });
            None
        }
    }
}
