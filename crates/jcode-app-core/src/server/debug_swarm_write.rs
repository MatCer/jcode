use super::{SharedContext, SwarmMember, SwarmState, VersionedPlan, persist_swarm_state_for};
use crate::plan::PlanItem;
use crate::protocol::{NotificationType, ServerEvent};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

pub(super) struct DebugSwarmWriteContext<'a> {
    pub(super) session_id: &'a Arc<RwLock<String>>,
    pub(super) sessions: &'a super::SessionAgents,
    pub(super) provider_template: &'a Arc<dyn crate::provider::Provider>,
    pub(super) swarm_members: &'a Arc<RwLock<HashMap<String, SwarmMember>>>,
    pub(super) swarms_by_id: &'a Arc<RwLock<HashMap<String, HashSet<String>>>>,
    pub(super) shared_context: &'a Arc<RwLock<HashMap<String, HashMap<String, SharedContext>>>>,
    pub(super) swarm_plans: &'a Arc<RwLock<HashMap<String, VersionedPlan>>>,
    pub(super) swarm_coordinators: &'a Arc<RwLock<HashMap<String, String>>>,
}

pub(super) async fn maybe_handle_swarm_write_command(
    cmd: &str,
    ctx: &DebugSwarmWriteContext<'_>,
) -> Result<Option<String>> {
    if cmd.starts_with("swarm:clear_coordinator:") {
        let swarm_id = cmd
            .strip_prefix("swarm:clear_coordinator:")
            .unwrap_or("")
            .trim();
        // Swarm member/coordinator mutations never nest these independent
        // locks. In particular, persistence reads coordinators again, so a
        // retained write guard here self-deadlocks the command.
        let removed = {
            let mut coordinators = ctx.swarm_coordinators.write().await;
            coordinators.remove(swarm_id).is_some()
        };
        if removed {
            {
                let mut members = ctx.swarm_members.write().await;
                for member in members.values_mut() {
                    if member.swarm_id.as_deref() == Some(swarm_id) && member.role == "coordinator"
                    {
                        member.role = "agent".to_string();
                    }
                }
            }
            let swarm_state = SwarmState {
                members: Arc::clone(ctx.swarm_members),
                swarms_by_id: Arc::clone(ctx.swarms_by_id),
                plans: Arc::clone(ctx.swarm_plans),
                coordinators: Arc::clone(ctx.swarm_coordinators),
            };
            persist_swarm_state_for(swarm_id, &swarm_state).await;
            return Ok(Some(format!(
                "Coordinator cleared for swarm '{}'. Any session can now self-promote.",
                swarm_id
            )));
        }
        return Err(anyhow::anyhow!(
            "No coordinator set for swarm '{}'",
            swarm_id
        ));
    }

    if cmd.starts_with("swarm:clear_plan:") {
        let swarm_id = cmd.strip_prefix("swarm:clear_plan:").unwrap_or("").trim();
        if swarm_id.is_empty() {
            return Err(anyhow::anyhow!(
                "swarm:clear_plan requires a swarm_id: swarm:clear_plan:<swarm_id>"
            ));
        }
        let removed = {
            let mut plans = ctx.swarm_plans.write().await;
            plans.remove(swarm_id)
        };
        let Some(removed) = removed else {
            return Err(anyhow::anyhow!("No plan found for swarm '{}'", swarm_id));
        };
        // Re-persist so the on-disk swarm state drops the plan too; otherwise
        // the next server restart resurrects it and every fresh session in
        // this working dir gets the stale plan graph pushed on subscribe.
        let swarm_state = SwarmState {
            members: Arc::clone(ctx.swarm_members),
            swarms_by_id: Arc::clone(ctx.swarms_by_id),
            plans: Arc::clone(ctx.swarm_plans),
            coordinators: Arc::clone(ctx.swarm_coordinators),
        };
        persist_swarm_state_for(swarm_id, &swarm_state).await;
        // Push the cleared state to attached clients. Without this, every
        // connected TUI keeps rendering (and holding resident) the old item
        // graph until its next reconnect; a 1.5k-item stale plan is ~650 KB
        // of JSON pinned per client. Version advances past the removed plan
        // so the client-side stale-regression guard accepts the update.
        let clear_event = ServerEvent::SwarmPlan {
            swarm_id: swarm_id.to_string(),
            version: removed.version.saturating_add(1),
            items: Vec::new(),
            participants: Vec::new(),
            reason: Some("plan_cleared".to_string()),
            summary: None,
        };
        let session_ids: Vec<String> = {
            let swarms = ctx.swarms_by_id.read().await;
            swarms
                .get(swarm_id)
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default()
        };
        {
            let members = ctx.swarm_members.read().await;
            for sid in session_ids {
                if let Some(member) = members.get(&sid) {
                    let _ = member.event_tx.send(clear_event.clone());
                    for tx in member.event_txs.values() {
                        let _ = tx.send(clear_event.clone());
                    }
                }
            }
        }
        return Ok(Some(
            serde_json::json!({
                "swarm_id": swarm_id,
                "cleared_version": removed.version,
                "cleared_item_count": removed.items.len(),
            })
            .to_string(),
        ));
    }

    if cmd.starts_with("swarm:broadcast:") {
        let rest = cmd.strip_prefix("swarm:broadcast:").unwrap_or("").trim();
        let (target_swarm_id, message) = if let Some(space_idx) = rest.find(' ') {
            let potential_id = &rest[..space_idx];
            let msg = rest[space_idx + 1..].trim();
            if potential_id.contains('/') {
                (Some(potential_id.to_string()), msg.to_string())
            } else {
                (None, rest.to_string())
            }
        } else {
            (None, rest.to_string())
        };

        if message.is_empty() {
            return Err(anyhow::anyhow!("swarm:broadcast requires a message"));
        }

        let swarm_id = if let Some(id) = target_swarm_id {
            Some(id)
        } else {
            let members = ctx.swarm_members.read().await;
            let current_session = ctx.session_id.read().await;
            members
                .get(&*current_session)
                .and_then(|member| member.swarm_id.clone())
        };

        if let Some(swarm_id) = swarm_id {
            let swarms = ctx.swarms_by_id.read().await;
            let members = ctx.swarm_members.read().await;
            let current_session = ctx.session_id.read().await;
            let from_name = members
                .get(&*current_session)
                .and_then(|member| member.friendly_name.clone());

            if let Some(member_ids) = swarms.get(&swarm_id) {
                let mut sent_count = 0;
                for member_id in member_ids {
                    if let Some(member) = members.get(member_id) {
                        let notification = ServerEvent::Notification {
                            from_session: current_session.clone(),
                            from_name: from_name.clone(),
                            notification_type: NotificationType::Message {
                                scope: Some("broadcast".to_string()),
                                channel: None,
                                tldr: None,
                            },
                            message: message.clone(),
                        };
                        if member.event_tx.send(notification).is_ok() {
                            sent_count += 1;
                        }
                    }
                }
                return Ok(Some(
                    serde_json::json!({
                        "swarm_id": swarm_id,
                        "message": message,
                        "sent_to": sent_count,
                    })
                    .to_string(),
                ));
            }

            return Err(anyhow::anyhow!("No members in swarm '{}'", swarm_id));
        }

        return Err(anyhow::anyhow!(
            "No swarm found. Specify swarm_id: swarm:broadcast:<swarm_id> <message>"
        ));
    }

    if cmd.starts_with("swarm:notify:") {
        let rest = cmd.strip_prefix("swarm:notify:").unwrap_or("").trim();
        if let Some(space_idx) = rest.find(' ') {
            let target_session = &rest[..space_idx];
            let message = rest[space_idx + 1..].trim();

            if message.is_empty() {
                return Err(anyhow::anyhow!("swarm:notify requires a message"));
            }

            let members = ctx.swarm_members.read().await;
            let current_session = ctx.session_id.read().await;
            let from_name = members
                .get(&*current_session)
                .and_then(|member| member.friendly_name.clone());

            if let Some(target) = members.get(target_session) {
                let notification = ServerEvent::Notification {
                    from_session: current_session.clone(),
                    from_name: from_name.clone(),
                    notification_type: NotificationType::Message {
                        scope: Some("dm".to_string()),
                        channel: None,
                        tldr: None,
                    },
                    message: message.to_string(),
                };
                if target.event_tx.send(notification).is_ok() {
                    return Ok(Some(
                        serde_json::json!({
                            "sent_to": target_session,
                            "sent_to_name": target.friendly_name.clone(),
                            "message": message,
                        })
                        .to_string(),
                    ));
                }
                return Err(anyhow::anyhow!("Failed to send notification"));
            }

            return Err(anyhow::anyhow!("Unknown session '{}'", target_session));
        }

        return Err(anyhow::anyhow!(
            "Usage: swarm:notify:<session_id> <message>"
        ));
    }

    if cmd.starts_with("swarm:set_context:") {
        let rest = cmd.strip_prefix("swarm:set_context:").unwrap_or("").trim();
        let parts: Vec<&str> = rest.splitn(3, ' ').collect();
        if parts.len() < 3 {
            return Err(anyhow::anyhow!(
                "Usage: swarm:set_context:<session_id> <key> <value>"
            ));
        }

        let acting_session = parts[0];
        let key = parts[1].to_string();
        let value = parts[2].to_string();

        let (swarm_id, friendly_name) = {
            let members = ctx.swarm_members.read().await;
            let swarm_id = members
                .get(acting_session)
                .and_then(|member| member.swarm_id.clone());
            let name = members
                .get(acting_session)
                .and_then(|member| member.friendly_name.clone());
            (swarm_id, name)
        };

        if let Some(swarm_id) = swarm_id {
            {
                let mut shared_ctx = ctx.shared_context.write().await;
                let swarm_ctx = shared_ctx
                    .entry(swarm_id.clone())
                    .or_insert_with(HashMap::new);
                let now = Instant::now();
                let created_at = swarm_ctx
                    .get(&key)
                    .map(|context| context.created_at)
                    .unwrap_or(now);
                swarm_ctx.insert(
                    key.clone(),
                    SharedContext {
                        key: key.clone(),
                        value: value.clone(),
                        from_session: acting_session.to_string(),
                        from_name: friendly_name.clone(),
                        created_at,
                        updated_at: now,
                    },
                );
            }

            let swarm_session_ids: Vec<String> = {
                let swarms = ctx.swarms_by_id.read().await;
                swarms
                    .get(&swarm_id)
                    .map(|sessions| sessions.iter().cloned().collect())
                    .unwrap_or_default()
            };
            let members = ctx.swarm_members.read().await;
            for sid in &swarm_session_ids {
                if sid != acting_session
                    && let Some(member) = members.get(sid)
                {
                    let _ = member.event_tx.send(ServerEvent::Notification {
                        from_session: acting_session.to_string(),
                        from_name: friendly_name.clone(),
                        notification_type: NotificationType::SharedContext {
                            key: key.clone(),
                            value: value.clone(),
                        },
                        message: format!("Shared context: {} = {}", key, value),
                    });
                }
            }

            return Ok(Some(
                serde_json::json!({
                    "swarm_id": swarm_id,
                    "key": key,
                    "value": value,
                    "from_session": acting_session,
                })
                .to_string(),
            ));
        }

        return Err(anyhow::anyhow!(
            "Session '{}' is not in a swarm",
            acting_session
        ));
    }

    if cmd.starts_with("swarm:approve_plan:") {
        let rest = cmd.strip_prefix("swarm:approve_plan:").unwrap_or("").trim();
        let parts: Vec<&str> = rest.splitn(2, ' ').collect();
        if parts.len() < 2 {
            return Err(anyhow::anyhow!(
                "Usage: swarm:approve_plan:<coordinator_session> <proposer_session>"
            ));
        }

        let coord_session = parts[0];
        let proposer_session = parts[1];

        let (swarm_id, is_coordinator) = {
            let members = ctx.swarm_members.read().await;
            let swarm_id = members
                .get(coord_session)
                .and_then(|member| member.swarm_id.clone());
            let is_coord = if let Some(ref swarm_id) = swarm_id {
                let coordinators = ctx.swarm_coordinators.read().await;
                coordinators
                    .get(swarm_id)
                    .map(|coordinator| coordinator == coord_session)
                    .unwrap_or(false)
            } else {
                false
            };
            (swarm_id, is_coord)
        };

        if !is_coordinator {
            return Err(anyhow::anyhow!(
                "Only the coordinator can approve plan proposals."
            ));
        }

        if let Some(swarm_id) = swarm_id {
            let proposal_key = format!("plan_proposal:{}", proposer_session);
            let proposal_value = {
                let shared_ctx = ctx.shared_context.read().await;
                shared_ctx
                    .get(&swarm_id)
                    .and_then(|swarm_ctx| swarm_ctx.get(&proposal_key))
                    .map(|context| context.value.clone())
            };

            return match proposal_value {
                None => Err(anyhow::anyhow!(
                    "No pending plan proposal from session '{}'",
                    proposer_session
                )),
                Some(proposal) => {
                    if let Ok(items) = serde_json::from_str::<Vec<PlanItem>>(&proposal) {
                        let version = {
                            let mut plans = ctx.swarm_plans.write().await;
                            let versioned_plan = plans
                                .entry(swarm_id.clone())
                                .or_insert_with(VersionedPlan::new);
                            let merged_count =
                                versioned_plan.items.len().saturating_add(items.len());
                            if merged_count > jcode_plan::MAX_PLAN_ITEMS {
                                return Err(anyhow::anyhow!(
                                    "Plan approval would contain {} items, exceeding the per-swarm limit of {}",
                                    merged_count,
                                    jcode_plan::MAX_PLAN_ITEMS
                                ));
                            }
                            versioned_plan.items.extend(items.clone());
                            versioned_plan.version += 1;
                            versioned_plan
                                .participants
                                .insert(coord_session.to_string());
                            versioned_plan
                                .participants
                                .insert(proposer_session.to_string());
                            versioned_plan.version
                        };
                        {
                            let mut shared_ctx = ctx.shared_context.write().await;
                            if let Some(swarm_ctx) = shared_ctx.get_mut(&swarm_id) {
                                swarm_ctx.remove(&proposal_key);
                            }
                        }
                        Ok(Some(
                            serde_json::json!({
                                "approved": true,
                                "items_added": items.len(),
                                "plan_version": version,
                                "swarm_id": swarm_id,
                            })
                            .to_string(),
                        ))
                    } else {
                        Err(anyhow::anyhow!(
                            "Failed to parse plan proposal as Vec<PlanItem>"
                        ))
                    }
                }
            };
        }

        return Err(anyhow::anyhow!("Not in a swarm."));
    }

    if cmd.starts_with("swarm:reject_plan:") {
        let rest = cmd.strip_prefix("swarm:reject_plan:").unwrap_or("").trim();
        let parts: Vec<&str> = rest.splitn(3, ' ').collect();
        if parts.len() < 2 {
            return Err(anyhow::anyhow!(
                "Usage: swarm:reject_plan:<coordinator_session> <proposer_session> [reason]"
            ));
        }

        let coord_session = parts[0];
        let proposer_session = parts[1];
        let reason = if parts.len() >= 3 {
            Some(parts[2].to_string())
        } else {
            None
        };

        let (swarm_id, is_coordinator) = {
            let members = ctx.swarm_members.read().await;
            let swarm_id = members
                .get(coord_session)
                .and_then(|member| member.swarm_id.clone());
            let is_coord = if let Some(ref swarm_id) = swarm_id {
                let coordinators = ctx.swarm_coordinators.read().await;
                coordinators
                    .get(swarm_id)
                    .map(|coordinator| coordinator == coord_session)
                    .unwrap_or(false)
            } else {
                false
            };
            (swarm_id, is_coord)
        };

        if !is_coordinator {
            return Err(anyhow::anyhow!(
                "Only the coordinator can reject plan proposals."
            ));
        }

        if let Some(swarm_id) = swarm_id {
            let proposal_key = format!("plan_proposal:{}", proposer_session);
            let proposal_exists = {
                let shared_ctx = ctx.shared_context.read().await;
                shared_ctx
                    .get(&swarm_id)
                    .and_then(|swarm_ctx| swarm_ctx.get(&proposal_key))
                    .is_some()
            };

            if !proposal_exists {
                return Err(anyhow::anyhow!(
                    "No pending plan proposal from session '{}'",
                    proposer_session
                ));
            }

            {
                let mut shared_ctx = ctx.shared_context.write().await;
                if let Some(swarm_ctx) = shared_ctx.get_mut(&swarm_id) {
                    swarm_ctx.remove(&proposal_key);
                }
            }
            let reason_msg = reason
                .as_ref()
                .map(|reason| format!(": {}", reason))
                .unwrap_or_default();
            return Ok(Some(
                serde_json::json!({
                    "rejected": true,
                    "proposer_session": proposer_session,
                    "reason": reason_msg,
                    "swarm_id": swarm_id,
                })
                .to_string(),
            ));
        }

        return Err(anyhow::anyhow!("Not in a swarm."));
    }

    // Task-DAG ops over the debug socket, for testing/operability without a live
    // model session. Arg is a JSON object:
    //   {"op":"seed","swarm_id":"..","mode":"deep","nodes":[{id,content,kind,depends_on}]}
    //   {"op":"expand","swarm_id":"..","actor":"sess","node_id":"..","children":[..]}
    //   {"op":"complete","swarm_id":"..","actor":"sess","node_id":"..","artifact":{..}}
    //   {"op":"inject","swarm_id":"..","actor":"sess","gate_id":"..","nodes":[..]}
    if let Some(rest) = cmd.strip_prefix("swarm:graph:") {
        return Ok(Some(handle_debug_graph_op(rest.trim(), ctx).await));
    }

    Ok(None)
}

#[derive(serde::Deserialize)]
struct DebugGraphArg {
    op: String,
    swarm_id: String,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    actor: Option<String>,
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    gate_id: Option<String>,
    #[serde(default)]
    nodes: Vec<DebugNodeSpec>,
    #[serde(default)]
    children: Vec<DebugNodeSpec>,
    #[serde(default)]
    artifact: Option<serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct DebugNodeSpec {
    id: String,
    content: String,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default)]
    priority: u8,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    effort: Option<String>,
}

fn debug_specs(specs: Vec<DebugNodeSpec>) -> Vec<jcode_plan::dag::NodeSpec> {
    specs
        .into_iter()
        .map(|s| jcode_plan::dag::NodeSpec {
            id: Some(s.id),
            content: s.content,
            kind: jcode_plan::bridge::parse_kind(s.kind.as_deref()),
            depends_on: s.depends_on,
            priority: s.priority,
            model: s.model,
            effort: s.effort,
        })
        .collect()
}

async fn handle_debug_graph_op(arg: &str, ctx: &DebugSwarmWriteContext<'_>) -> String {
    use jcode_plan::bridge::{apply_task_graph, to_task_graph};
    use jcode_plan::dag;

    fn fail(msg: impl std::fmt::Display) -> String {
        serde_json::json!({"ok": false, "error": msg.to_string()}).to_string()
    }

    let parsed: DebugGraphArg = match serde_json::from_str(arg) {
        Ok(parsed) => parsed,
        Err(e) => return fail(format!("invalid swarm:graph JSON: {e}")),
    };
    let swarm_id = parsed.swarm_id.clone();

    let actor = parsed.actor.unwrap_or_else(|| String::new());
    let actor = if actor.is_empty() {
        ctx.session_id.read().await.clone()
    } else {
        actor
    };
    if !ctx
        .swarm_members
        .read()
        .await
        .get(&actor)
        .is_some_and(|member| member.swarm_id.as_deref() == Some(&swarm_id))
    {
        return fail("Actor must be an existing member of this swarm.");
    }
    let admitted_plan = ctx
        .swarm_plans
        .read()
        .await
        .get(&swarm_id)
        .cloned()
        .unwrap_or_default();
    let mut staged_plan = admitted_plan.clone();
    let node_id = if parsed.op == "inject" {
        parsed.gate_id.as_deref()
    } else {
        parsed.node_id.as_deref()
    };
    let actor_routing = if parsed.op != "seed" {
        let Some(node_id) = node_id else {
            return fail("node_id/gate_id required");
        };
        match super::comm_graph::validate_graph_actor(
            &actor,
            node_id,
            &admitted_plan,
            ctx.sessions,
            ctx.swarm_members,
            ctx.provider_template,
        )
        .await
        {
            Ok(routing) => routing,
            Err(error) => return fail(error),
        }
    } else {
        None
    };
    if let Some(mode) = parsed.mode {
        if admitted_plan.mode.eq_ignore_ascii_case("deep")
            && !mode.eq_ignore_ascii_case("deep")
            && !admitted_plan.items.is_empty()
        {
            return fail("Cannot downgrade a non-empty deep plan.");
        }
        staged_plan.mode = mode;
    }
    let specs = debug_specs(if parsed.op == "expand" {
        parsed.children
    } else {
        parsed.nodes
    });
    let parent = node_id
        .and_then(|id| admitted_plan.node_meta.get(id))
        .cloned()
        .unwrap_or_default();
    let generates_gate = matches!(parsed.op.as_str(), "seed" | "expand")
        && staged_plan.mode.eq_ignore_ascii_case("deep");
    let allowed = if parsed.op != "complete" {
        match super::comm_graph::validate_graph_specs(
            &actor,
            &specs,
            &parent,
            generates_gate,
            ctx.sessions,
            ctx.swarm_members,
            ctx.provider_template,
        )
        .await
        {
            Ok(allowed) => allowed,
            Err(error) => return fail(error),
        }
    } else {
        parent.allowed_models.clone()
    };
    let count = if parsed.op == "complete" {
        1
    } else {
        specs.len()
    };
    let mut graph = to_task_graph(&staged_plan);
    let before = graph.clone();
    if let Some(node_id) = node_id {
        super::comm_graph::pin_graph_actor(&mut graph, node_id, actor_routing.as_ref());
        super::comm_graph::claim_queued_node_for_actor(&mut graph, node_id, &actor);
    }
    let result = match parsed.op.as_str() {
        "seed" => dag::seed(&mut graph, specs),
        "expand" => dag::expand_node(&mut graph, node_id.unwrap(), &actor, specs).map(|_| ()),
        "inject" => dag::inject_from_gate(&mut graph, node_id.unwrap(), &actor, specs).map(|_| ()),
        "complete" => {
            let artifact =
                match serde_json::from_value(parsed.artifact.unwrap_or(serde_json::json!({}))) {
                    Ok(artifact) => artifact,
                    Err(error) => return fail(format!("invalid artifact: {error}")),
                };
            dag::complete_node(&mut graph, node_id.unwrap(), &actor, artifact)
        }
        other => return fail(format!("unknown op '{other}'")),
    };
    let result: Result<(usize, &str), String> = match result {
        Err(error) => Err(error.to_string()),
        Ok(()) if graph.len() > jcode_plan::MAX_PLAN_ITEMS => {
            Err("Graph exceeds per-swarm node limit".into())
        }
        Ok(()) => {
            super::comm_graph::restrict_new_graph_nodes(&mut graph, &before, allowed.as_deref());
            apply_task_graph(&mut staged_plan, &graph);
            staged_plan.version += 1;
            let mut plans = ctx.swarm_plans.write().await;
            if plans.get(&swarm_id).map_or(0, |plan| plan.version) != admitted_plan.version {
                return fail("Plan changed during admission; retry graph operation.");
            }
            plans.insert(swarm_id.clone(), staged_plan);
            Ok((count, parsed.op.as_str()))
        }
    };

    match result {
        Ok((count, op)) => {
            let swarm_state = SwarmState {
                members: Arc::clone(ctx.swarm_members),
                swarms_by_id: Arc::clone(ctx.swarms_by_id),
                plans: Arc::clone(ctx.swarm_plans),
                coordinators: Arc::clone(ctx.swarm_coordinators),
            };
            persist_swarm_state_for(&swarm_id, &swarm_state).await;
            serde_json::json!({"ok": true, "op": op, "count": count, "swarm_id": swarm_id})
                .to_string()
        }
        Err(e) => fail(format!("graph op rejected: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn routing_debug_specs_keep_requests_but_not_spoofed_policy_or_pin() {
        let specs: Vec<DebugNodeSpec> = serde_json::from_value(serde_json::json!([{
            "id":"a", "content":"work", "model":"gpt-6-astra", "effort":"medium",
            "allowed_models":[], "resolved_execution":{"model":"spoof"}
        }]))
        .unwrap();
        let specs = debug_specs(specs);
        assert_eq!(specs[0].model.as_deref(), Some("gpt-6-astra"));
        assert_eq!(specs[0].effort.as_deref(), Some("medium"));
        let mut graph = jcode_plan::dag::TaskGraph::new(jcode_plan::dag::Mode::Light);
        jcode_plan::dag::seed(&mut graph, specs).unwrap();
        assert!(graph.get("a").unwrap().allowed_models.is_none());
        assert!(graph.get("a").unwrap().resolved_execution.is_none());
    }

    struct TestProvider;
    #[async_trait::async_trait]
    impl crate::provider::Provider for TestProvider {
        async fn complete(
            &self,
            _: &[crate::message::Message],
            _: &[crate::message::ToolDefinition],
            _: &str,
            _: Option<&str>,
        ) -> anyhow::Result<crate::provider::EventStream> {
            Ok(Box::pin(futures::stream::empty()))
        }
        fn name(&self) -> &str {
            "test"
        }
        fn fork(&self) -> Arc<dyn crate::provider::Provider> {
            Arc::new(Self)
        }
    }

    struct EnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        runtime: Option<std::ffi::OsString>,
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(value) = self.runtime.take() {
                crate::env::set_var("JCODE_RUNTIME_DIR", value);
            } else {
                crate::env::remove_var("JCODE_RUNTIME_DIR");
            }
        }
    }

    fn isolated_runtime(dir: &tempfile::TempDir) -> EnvGuard {
        let lock = crate::storage::lock_test_env();
        let runtime = std::env::var_os("JCODE_RUNTIME_DIR");
        crate::env::set_var("JCODE_RUNTIME_DIR", dir.path());
        EnvGuard {
            _lock: lock,
            runtime,
        }
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn clear_coordinator_releases_coordinator_lock_before_waiting_for_members() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let _env = isolated_runtime(&dir);
        let session_id = Arc::new(RwLock::new("session-1".to_string()));
        let swarm_members = Arc::new(RwLock::new(HashMap::new()));
        let swarms_by_id = Arc::new(RwLock::new(HashMap::new()));
        let shared_context = Arc::new(RwLock::new(HashMap::new()));
        let swarm_plans = Arc::new(RwLock::new(HashMap::new()));
        let swarm_coordinators = Arc::new(RwLock::new(HashMap::from([(
            "swarm-lock-order".to_string(),
            "session-1".to_string(),
        )])));
        let ctx = DebugSwarmWriteContext {
            session_id: &session_id,
            sessions: &Arc::new(RwLock::new(HashMap::new())),
            provider_template: &(Arc::new(TestProvider) as Arc<dyn crate::provider::Provider>),
            swarm_members: &swarm_members,
            swarms_by_id: &swarms_by_id,
            shared_context: &shared_context,
            swarm_plans: &swarm_plans,
            swarm_coordinators: &swarm_coordinators,
        };

        // Force the command to wait at members.write(). A safe path must not
        // retain coordinators.write() while it waits for that independent lock.
        let members_gate = swarm_members.write().await;
        let command =
            maybe_handle_swarm_write_command("swarm:clear_coordinator:swarm-lock-order", &ctx);
        tokio::pin!(command);
        tokio::select! {
            result = &mut command => panic!("command unexpectedly completed: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }

        let coordinators =
            tokio::time::timeout(Duration::from_millis(100), swarm_coordinators.read())
                .await
                .expect("coordinator lock was retained while waiting for members");
        assert!(!coordinators.contains_key("swarm-lock-order"));
        drop(coordinators);

        drop(members_gate);
        let response = tokio::time::timeout(Duration::from_secs(1), &mut command)
            .await
            .expect("clear coordinator self-deadlocked")
            .expect("command failed")
            .expect("command was not handled");
        assert!(response.contains("Coordinator cleared"));
    }
}
