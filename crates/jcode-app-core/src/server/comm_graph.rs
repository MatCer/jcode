//! Server handlers for the task-DAG mutation ops (seed/expand/complete/inject).
//!
//! These are the live counterparts of the validated engine ops in
//! `jcode_plan::dag`. Each handler lifts the swarm's current `VersionedPlan` into
//! a `TaskGraph` (via `jcode_plan::bridge`), applies the engine op (which enforces
//! acyclicity, ownership, gate insertion, and artifact validation), lowers the
//! result back into the plan, then persists and broadcasts using the existing
//! swarm machinery. This keeps a single source of truth and reuses the scheduler,
//! persistence, and TUI broadcast paths.

use super::{
    SwarmEvent, SwarmEventType, SwarmMember, SwarmState, VersionedPlan, broadcast_swarm_plan,
    persist_swarm_state_for, record_swarm_event,
};
use crate::protocol::ServerEvent;
use crate::protocol::TaskGraphNodeSpec;
use jcode_plan::MAX_PLAN_ITEMS;
use jcode_plan::bridge::{apply_task_graph, parse_kind, to_task_graph};
use jcode_plan::dag::{self, HandoffArtifact, NodeSpec, NodeStatus, TaskGraph};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::{RwLock, broadcast};

fn spec_from_wire(spec: TaskGraphNodeSpec) -> NodeSpec {
    NodeSpec {
        id: Some(spec.id),
        content: spec.content,
        kind: parse_kind(spec.kind.as_deref()),
        depends_on: spec.depends_on,
        priority: spec.priority,
        model: spec.model,
        effort: spec.effort,
    }
}

/// Admission is read-only. A failed batch never creates nodes, elects a
/// coordinator, or claims a queued parent. Generated gates use default routing
/// but inherit the same trusted restrictions as the actor's new children.
pub(super) async fn validate_graph_specs(
    actor: &str,
    specs: &[NodeSpec],
    parent: &jcode_plan::NodeMeta,
    generates_gate: bool,
    sessions: &super::SessionAgents,
    members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    provider: &Arc<dyn crate::provider::Provider>,
) -> anyhow::Result<Option<Vec<String>>> {
    let mut allowed = parent.allowed_models.clone();
    for spec in specs {
        let meta = jcode_plan::NodeMeta {
            model: spec.model.clone(),
            effort: spec.effort.clone(),
            allowed_models: parent.allowed_models.clone(),
            ..Default::default()
        };
        let routing = super::comm_control::admit_node_execution(
            actor, &meta, None, None, None, sessions, members, provider,
        )
        .await?;
        allowed = routing.allowed_models;
    }
    if generates_gate {
        let meta = jcode_plan::NodeMeta {
            allowed_models: allowed.clone(),
            ..Default::default()
        };
        allowed = super::comm_control::admit_node_execution(
            actor, &meta, None, None, None, sessions, members, provider,
        )
        .await?
        .allowed_models;
    }
    Ok(allowed)
}

pub(super) fn restrict_new_graph_nodes(
    graph: &mut TaskGraph,
    before: &TaskGraph,
    allowed: Option<&[String]>,
) {
    let mut updated = TaskGraph::new(graph.mode);
    for mut node in graph.nodes().iter().cloned() {
        if !before.contains(&node.id) {
            node.allowed_models = allowed.map(<[String]>::to_vec);
        }
        updated.push_node(node);
    }
    *graph = updated;
}

/// Self-claiming a queued node is an execution boundary too. Already running
/// work can submit its artifact after policy tightening without being revoked.
pub(super) async fn validate_graph_actor(
    actor: &str,
    node_id: &str,
    plan: &VersionedPlan,
    sessions: &super::SessionAgents,
    members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    provider: &Arc<dyn crate::provider::Provider>,
) -> anyhow::Result<Option<crate::protocol::SwarmRoutingStatus>> {
    let graph = to_task_graph(plan);
    let node = graph
        .get(node_id)
        .ok_or_else(|| anyhow::anyhow!("Unknown node '{node_id}'"))?;
    if node.status != NodeStatus::Queued {
        return Ok(None);
    }
    if node.owner.as_deref().is_some_and(|owner| owner != actor) {
        anyhow::bail!("Actor does not own node '{node_id}'.");
    }
    // Claiming assigned work is not recursive spawning. Resolve its policy
    // through the trusted root plus the node snapshot and actor restrictions.
    let ancestry = super::swarm::trusted_swarm_ancestry(&*members.read().await, actor)?;
    let root = ancestry
        .last()
        .ok_or_else(|| anyhow::anyhow!("Missing trusted root"))?;
    let meta = plan.node_meta.get(node_id).cloned().unwrap_or_default();
    let mut routing = super::comm_control::admit_node_execution(
        root, &meta, None, None, None, sessions, members, provider,
    )
    .await?;
    let actor_allowed = super::comm_session::effective_swarm_allowed_models(
        &*members.read().await,
        actor,
        None,
        None,
        &provider.model_routes(),
    )?;
    routing.allowed_models = super::comm_control::intersect_node_allowed_models(
        routing.allowed_models.as_deref(),
        actor_allowed.as_deref(),
        &provider.model_routes(),
    );
    let compatible = super::comm_control::compatible_node_candidates(
        &routing,
        meta.resolved_execution.is_some(),
        sessions,
        members,
        provider,
    )
    .await;
    routing.actual_execution = compatible.get(actor).cloned().ok_or_else(||
        anyhow::anyhow!("Actor '{actor}' has no compatible initialized model/provider/API/effort for node '{node_id}'."))?;
    Ok(Some(routing))
}

pub(super) struct GraphActorGuard<'a> {
    _agent: tokio::sync::OwnedMutexGuard<crate::agent::Agent>,
    _members: tokio::sync::RwLockReadGuard<'a, HashMap<String, SwarmMember>>,
    _sessions: tokio::sync::RwLockReadGuard<
        'a,
        HashMap<String, Arc<tokio::sync::Mutex<crate::agent::Agent>>>,
    >,
}

/// Called under the final plan lock, never across an await. A running actor
/// submitting an artifact is not a new execution and is not revoked here.
pub(super) fn bind_graph_actor<'a>(
    actor: &str,
    pinned: bool,
    routing: &mut Option<crate::protocol::SwarmRoutingStatus>,
    sessions: &'a super::SessionAgents,
    members: &'a Arc<RwLock<HashMap<String, SwarmMember>>>,
    provider: &Arc<dyn crate::provider::Provider>,
) -> anyhow::Result<Option<GraphActorGuard<'a>>> {
    let Some(routing) = routing else {
        return Ok(None);
    };
    let sessions = sessions
        .try_read()
        .map_err(|_| anyhow::anyhow!("Session map is busy; retry graph operation."))?;
    let agent = sessions
        .get(actor)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Actor is no longer initialized."))?
        .try_lock_owned()
        .map_err(|_| anyhow::anyhow!("Actor is busy; retry graph operation when idle."))?;
    let members = members
        .try_read()
        .map_err(|_| anyhow::anyhow!("Member policy is busy; retry graph operation."))?;
    let ancestry = super::swarm::trusted_swarm_ancestry(&members, actor)?;
    let root = ancestry
        .last()
        .ok_or_else(|| anyhow::anyhow!("Missing trusted root"))?;
    *routing = super::comm_control::observe_node_execution(
        root,
        actor,
        routing,
        pinned,
        &agent,
        &members,
        provider.as_ref(),
    )?;
    Ok(Some(GraphActorGuard {
        _agent: agent,
        _members: members,
        _sessions: sessions,
    }))
}

pub(super) fn pin_graph_actor(
    graph: &mut TaskGraph,
    node_id: &str,
    routing: Option<&crate::protocol::SwarmRoutingStatus>,
) {
    let Some(routing) = routing else { return };
    let mut updated = TaskGraph::new(graph.mode);
    for mut node in graph.nodes().iter().cloned() {
        if node.id == node_id {
            if node.resolved_execution.is_none() {
                node.resolved_execution = routing.actual_execution.clone();
            }
            node.allowed_models = routing.allowed_models.clone();
        }
        updated.push_node(node);
    }
    *graph = updated;
}

#[cfg(test)]
mod routing_tests {
    use super::*;

    #[test]
    fn routing_graph_adapter_preserves_requested_model_and_effort_only() {
        let wire: TaskGraphNodeSpec = serde_json::from_value(serde_json::json!({
            "id":"a", "content":"work", "model":"gpt-6-astra", "effort":"medium",
            "resolved_execution":{"model":"spoof"}, "allowed_models":[]
        }))
        .unwrap();
        let spec = spec_from_wire(wire);
        assert_eq!(spec.model.as_deref(), Some("gpt-6-astra"));
        assert_eq!(spec.effort.as_deref(), Some("medium"));
        let mut graph = TaskGraph::new(dag::Mode::Light);
        dag::seed(&mut graph, vec![spec]).unwrap();
        let node = graph.get("a").unwrap();
        assert!(node.resolved_execution.is_none());
        assert!(node.allowed_models.is_none());
    }
}

fn graph_size_error(graph: &TaskGraph) -> Option<String> {
    (graph.len() > MAX_PLAN_ITEMS).then(|| {
        format!(
            "plan would contain {} items, exceeding the per-swarm limit of {}; finish or clear stale plan nodes before adding more",
            graph.len(),
            MAX_PLAN_ITEMS
        )
    })
}

async fn swarm_id_for(
    session_id: &str,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
) -> Option<String> {
    swarm_members
        .read()
        .await
        .get(session_id)
        .and_then(|member| member.swarm_id.clone())
}

/// Ensure the seeding session can actually drive the graph it just created.
///
/// Deep-mode sessions are frequently solo `agent`s with no coordinator elected,
/// yet `assign_task` / `assign_next` / `run_plan` are coordinator-gated. Without
/// this, a fresh deep-mode agent can seed a task graph but then cannot dispatch
/// any of it. We elect the seeder as coordinator when the swarm has no *live*
/// coordinator, mirroring the self-promote rule used by `assign_role`. A live,
/// non-headless coordinator is left untouched so a real coordinator is never
/// displaced by a worker that happens to seed.
///
/// Returns true when the seeder was (or already is) the coordinator afterwards.
async fn ensure_seeder_can_coordinate(
    swarm_id: &str,
    seeder_session_id: &str,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
) -> bool {
    // 1. Read the current coordinator id without holding the lock across the
    //    liveness check (matches the non-nested lock pattern used elsewhere).
    let current = swarm_coordinators.read().await.get(swarm_id).cloned();
    match &current {
        Some(coord) if coord == seeder_session_id => return true,
        _ => {}
    }

    // 2. Decide whether the existing coordinator is still a live driver.
    let coordinator_is_live = match &current {
        Some(coord) => {
            let members = swarm_members.read().await;
            members
                .get(coord)
                .map(|member| !member.event_tx.is_closed() && !member.is_headless)
                .unwrap_or(false)
        }
        None => false,
    };
    if coordinator_is_live {
        return false;
    }

    // 3. Promote the seeder; demote any prior (stale) coordinator member. Re-check
    //    under the write lock that the coordinator is still the one we inspected
    //    (compare-and-swap): two concurrent seeders race here, and the loser must
    //    not silently displace the winner it never liveness-checked.
    let prior = {
        let mut coordinators = swarm_coordinators.write().await;
        if coordinators.get(swarm_id) != current.as_ref() {
            // Someone else changed the coordinator between our read and write.
            return coordinators.get(swarm_id).map(String::as_str) == Some(seeder_session_id);
        }
        coordinators.insert(swarm_id.to_string(), seeder_session_id.to_string())
    };
    {
        let mut members = swarm_members.write().await;
        if let Some(member) = members.get_mut(seeder_session_id) {
            member.role = "coordinator".to_string();
        }
        if let Some(prior) = prior
            && prior != seeder_session_id
            && let Some(member) = members.get_mut(&prior)
        {
            member.role = "agent".to_string();
        }
    }
    true
}

/// Auto-claim a queued node for the participant that is trying to mutate it.
///
/// Seeded nodes are unowned until dispatch, but the deep-mode contract tells the
/// seeding agent to `expand_node`/`complete_node` its own nodes, and the assign
/// path refuses self-assignment — so without this a solo deep seeder could never
/// legally touch any node it seeded (observed live as "Complete rejected: actor
/// does not own node"). Similarly, assignment to a client-attached worker leaves
/// the item `queued` (the server-run flip to `running` is skipped when a live
/// client owns the turn), so the assignee's own complete/expand would bounce with
/// "invalid state Queued".
///
/// Claiming is safe only when the node is genuinely available to this actor:
/// queued, with every dependency done (enforced by `dispatch`), and either
/// unowned or already assigned to this same actor. A node owned by someone else
/// is never touched — the engine's `NotOwner` check still applies.
pub(super) fn claim_queued_node_for_actor(graph: &mut TaskGraph, node_id: &str, actor: &str) {
    let claimable = graph.get(node_id).is_some_and(|node| {
        node.status == NodeStatus::Queued
            && node.owner.as_deref().is_none_or(|owner| owner == actor)
    });
    if claimable {
        // `dispatch` re-validates queued status and dependency satisfaction; if
        // deps are not done the claim is skipped and the engine op reports the
        // real error.
        let _ = dag::dispatch(graph, node_id, actor);
    }
}

fn err(client_event_tx: &mpsc::UnboundedSender<ServerEvent>, id: u64, message: String) {
    let _ = client_event_tx.send(ServerEvent::Error {
        id,
        message,
        retry_after_secs: None,
    });
}

/// Shared finalize: persist, broadcast, record a plan-update event, and ack.
#[expect(
    clippy::too_many_arguments,
    reason = "finalize threads through swarm persistence, broadcast, and event-history handles"
)]
async fn finalize(
    id: u64,
    swarm_id: &str,
    req_session_id: &str,
    reason: &str,
    item_count: usize,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    swarm_plans: &Arc<RwLock<HashMap<String, VersionedPlan>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
) {
    let from_name = swarm_members
        .read()
        .await
        .get(req_session_id)
        .and_then(|member| member.friendly_name.clone());

    let swarm_state = SwarmState {
        members: Arc::clone(swarm_members),
        swarms_by_id: Arc::clone(swarms_by_id),
        plans: Arc::clone(swarm_plans),
        coordinators: Arc::clone(swarm_coordinators),
    };
    persist_swarm_state_for(swarm_id, &swarm_state).await;
    broadcast_swarm_plan(
        swarm_id,
        Some(reason.to_string()),
        swarm_plans,
        swarm_members,
        swarms_by_id,
    )
    .await;
    record_swarm_event(
        event_history,
        event_counter,
        swarm_event_tx,
        req_session_id.to_string(),
        from_name,
        Some(swarm_id.to_string()),
        SwarmEventType::PlanUpdate {
            swarm_id: swarm_id.to_string(),
            item_count,
        },
    )
    .await;
    let _ = client_event_tx.send(ServerEvent::Done { id });
}

/// Seed (or re-seed) the swarm task DAG from a batch of node specs.
#[expect(
    clippy::too_many_arguments,
    reason = "swarm op threads runtime handles"
)]
pub(super) async fn handle_comm_seed_graph(
    id: u64,
    req_session_id: String,
    mode: Option<String>,
    nodes: Vec<TaskGraphNodeSpec>,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    swarm_plans: &Arc<RwLock<HashMap<String, VersionedPlan>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
    sessions: &super::SessionAgents,
    provider_template: &Arc<dyn crate::provider::Provider>,
) {
    let Some(swarm_id) = swarm_id_for(&req_session_id, swarm_members).await else {
        err(client_event_tx, id, "Not in a swarm.".to_string());
        return;
    };

    let specs: Vec<NodeSpec> = nodes.into_iter().map(spec_from_wire).collect();
    let count = specs.len();

    // Resolve the plan mode. The model is *asked* to pass `mode:"deep"` when it is
    // running at `swarm-deep` effort, but it frequently forgets. Rather than
    // silently downgrading a deep-effort session to light (which disables the
    // gates + artifact validation that define deep mode), default the mode from
    // the seeder's recorded reasoning effort when the caller did not specify one.
    // An explicit `mode` always wins so a caller can still opt into light.
    let resolved_mode = mode.or_else(|| {
        crate::session_effort::session_effort(&req_session_id)
            .filter(|effort| crate::prompt::is_deep_swarm_effort(effort))
            .map(|_| "deep".to_string())
    });

    let current_mode = swarm_plans
        .read()
        .await
        .get(&swarm_id)
        .map(|plan| plan.mode.clone())
        .unwrap_or_else(|| "light".into());
    let generates_gate = resolved_mode
        .as_deref()
        .unwrap_or(&current_mode)
        .eq_ignore_ascii_case("deep");
    let allowed = match validate_graph_specs(
        &req_session_id,
        &specs,
        &jcode_plan::NodeMeta::default(),
        generates_gate,
        sessions,
        swarm_members,
        provider_template,
    )
    .await
    {
        Ok(allowed) => allowed,
        Err(error) => {
            err(
                client_event_tx,
                id,
                format!("Seed routing rejected: {error}"),
            );
            return;
        }
    };
    let result = {
        let mut plans = swarm_plans.write().await;
        let mut staged_plan = plans.get(&swarm_id).cloned().unwrap_or_default();
        let plan = &mut staged_plan;
        if let Some(mode) = resolved_mode {
            // Guard against silent rigor downgrades: re-seeding an existing deep
            // plan as light would strip the gates + artifact validation from all
            // nodes already in flight. Deepening (light -> deep) or re-stating
            // the same mode is fine; only the downgrade of a non-empty deep plan
            // is rejected.
            let downgrades_deep = plan.mode.eq_ignore_ascii_case("deep")
                && !mode.eq_ignore_ascii_case("deep")
                && !plan.items.is_empty();
            if downgrades_deep {
                err(
                    client_event_tx,
                    id,
                    "Seed rejected: this swarm already has a non-empty deep-mode plan; \
                     seeding with mode=light would silently strip its gates and artifact \
                     validation. Omit `mode` to keep deep, or finish/clear the current plan first."
                        .to_string(),
                );
                return;
            }
            plan.mode = mode;
        }
        plan.participants.insert(req_session_id.clone());
        let mut graph = to_task_graph(plan);
        let before = graph.clone();
        match dag::seed(&mut graph, specs) {
            Ok(()) => match graph_size_error(&graph) {
                Some(message) => Err(message),
                None => {
                    if graph != before {
                        restrict_new_graph_nodes(&mut graph, &before, allowed.as_deref());
                        apply_task_graph(plan, &graph);
                        plan.version += 1;
                    }
                    plans.insert(swarm_id.clone(), staged_plan);
                    Ok(())
                }
            },
            Err(e) => Err(e.to_string()),
        }
    };

    match result {
        Ok(()) => {
            // A deep-mode seeder is usually a solo agent. Elect it coordinator (when no
            // live coordinator exists) so it can actually dispatch the graph it seeds via
            // the coordinator-gated assign/run_plan paths.
            ensure_seeder_can_coordinate(
                &swarm_id,
                &req_session_id,
                swarm_members,
                swarm_coordinators,
            )
            .await;

            finalize(
                id,
                &swarm_id,
                &req_session_id,
                "task_graph_seed",
                count,
                client_event_tx,
                swarm_members,
                swarms_by_id,
                swarm_plans,
                swarm_coordinators,
                event_history,
                event_counter,
                swarm_event_tx,
            )
            .await;
        }
        Err(e) => err(client_event_tx, id, format!("Seed rejected: {e}")),
    }
}

/// Decompose a node the caller owns into a child sub-DAG.
#[expect(
    clippy::too_many_arguments,
    reason = "swarm op threads runtime handles"
)]
pub(super) async fn handle_comm_expand_node(
    id: u64,
    req_session_id: String,
    node_id: String,
    children: Vec<TaskGraphNodeSpec>,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    swarm_plans: &Arc<RwLock<HashMap<String, VersionedPlan>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
    sessions: &super::SessionAgents,
    provider_template: &Arc<dyn crate::provider::Provider>,
) {
    let Some(swarm_id) = swarm_id_for(&req_session_id, swarm_members).await else {
        err(client_event_tx, id, "Not in a swarm.".to_string());
        return;
    };
    let specs: Vec<NodeSpec> = children.into_iter().map(spec_from_wire).collect();
    let count = specs.len();

    let Some(admitted_plan) = swarm_plans.read().await.get(&swarm_id).cloned() else {
        err(client_event_tx, id, "No plan for this swarm.".into());
        return;
    };
    let mut actor_routing = match validate_graph_actor(
        &req_session_id,
        &node_id,
        &admitted_plan,
        sessions,
        swarm_members,
        provider_template,
    )
    .await
    {
        Ok(routing) => routing,
        Err(error) => {
            err(
                client_event_tx,
                id,
                format!("Node execution rejected: {error}"),
            );
            return;
        }
    };
    let parent = admitted_plan
        .node_meta
        .get(&node_id)
        .cloned()
        .unwrap_or_default();
    let allowed = match validate_graph_specs(
        &req_session_id,
        &specs,
        &parent,
        true && admitted_plan.mode.eq_ignore_ascii_case("deep"),
        sessions,
        swarm_members,
        provider_template,
    )
    .await
    {
        Ok(allowed) => allowed,
        Err(error) => {
            err(
                client_event_tx,
                id,
                format!("Graph routing rejected: {error}"),
            );
            return;
        }
    };
    let result = {
        let mut plans = swarm_plans.write().await;
        let Some(plan) = plans.get_mut(&swarm_id) else {
            err(client_event_tx, id, "No plan for this swarm.".to_string());
            return;
        };
        if plan.version != admitted_plan.version {
            err(
                client_event_tx,
                id,
                "Plan changed during admission; retry graph operation.".into(),
            );
            return;
        }
        let _execution_guard = match bind_graph_actor(
            &req_session_id,
            plan.node_meta
                .get(&node_id)
                .is_some_and(|meta| meta.resolved_execution.is_some()),
            &mut actor_routing,
            sessions,
            swarm_members,
            provider_template,
        ) {
            Ok(guard) => guard,
            Err(error) => {
                err(
                    client_event_tx,
                    id,
                    format!("Node execution rejected: {error}"),
                );
                return;
            }
        };
        let mut graph = to_task_graph(plan);
        let before = graph.clone();
        pin_graph_actor(&mut graph, &node_id, actor_routing.as_ref());
        claim_queued_node_for_actor(&mut graph, &node_id, &req_session_id);
        match dag::expand_node(&mut graph, &node_id, &req_session_id, specs) {
            Ok(_) => match graph_size_error(&graph) {
                Some(message) => Err(message),
                None => {
                    restrict_new_graph_nodes(&mut graph, &before, allowed.as_deref());
                    apply_task_graph(plan, &graph);
                    plan.version += 1;
                    Ok(())
                }
            },
            Err(e) => Err(e.to_string()),
        }
    };

    match result {
        Ok(()) => {
            finalize(
                id,
                &swarm_id,
                &req_session_id,
                "task_graph_expand",
                count,
                client_event_tx,
                swarm_members,
                swarms_by_id,
                swarm_plans,
                swarm_coordinators,
                event_history,
                event_counter,
                swarm_event_tx,
            )
            .await;
        }
        Err(e) => err(client_event_tx, id, format!("Expand rejected: {e}")),
    }
}

/// Complete a node the caller owns with a typed handoff artifact.
#[expect(
    clippy::too_many_arguments,
    reason = "swarm op threads runtime handles"
)]
pub(super) async fn handle_comm_complete_node(
    id: u64,
    req_session_id: String,
    node_id: String,
    artifact_json: String,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    swarm_plans: &Arc<RwLock<HashMap<String, VersionedPlan>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
    sessions: &super::SessionAgents,
    provider_template: &Arc<dyn crate::provider::Provider>,
) {
    let Some(swarm_id) = swarm_id_for(&req_session_id, swarm_members).await else {
        err(client_event_tx, id, "Not in a swarm.".to_string());
        return;
    };

    let artifact: HandoffArtifact = match serde_json::from_str(&artifact_json) {
        Ok(artifact) => artifact,
        Err(e) => {
            err(client_event_tx, id, format!("Invalid artifact JSON: {e}"));
            return;
        }
    };

    let Some(admitted_plan) = swarm_plans.read().await.get(&swarm_id).cloned() else {
        err(client_event_tx, id, "No plan for this swarm.".into());
        return;
    };
    let mut actor_routing = match validate_graph_actor(
        &req_session_id,
        &node_id,
        &admitted_plan,
        sessions,
        swarm_members,
        provider_template,
    )
    .await
    {
        Ok(routing) => routing,
        Err(error) => {
            err(
                client_event_tx,
                id,
                format!("Node execution rejected: {error}"),
            );
            return;
        }
    };
    let result = {
        let mut plans = swarm_plans.write().await;
        let Some(plan) = plans.get_mut(&swarm_id) else {
            err(client_event_tx, id, "No plan for this swarm.".to_string());
            return;
        };
        if plan.version != admitted_plan.version {
            err(
                client_event_tx,
                id,
                "Plan changed during admission; retry graph operation.".into(),
            );
            return;
        }
        let _execution_guard = match bind_graph_actor(
            &req_session_id,
            plan.node_meta
                .get(&node_id)
                .is_some_and(|meta| meta.resolved_execution.is_some()),
            &mut actor_routing,
            sessions,
            swarm_members,
            provider_template,
        ) {
            Ok(guard) => guard,
            Err(error) => {
                err(
                    client_event_tx,
                    id,
                    format!("Node execution rejected: {error}"),
                );
                return;
            }
        };
        let mut graph = to_task_graph(plan);
        pin_graph_actor(&mut graph, &node_id, actor_routing.as_ref());
        claim_queued_node_for_actor(&mut graph, &node_id, &req_session_id);
        match dag::complete_node(&mut graph, &node_id, &req_session_id, artifact) {
            Ok(()) => {
                apply_task_graph(plan, &graph);
                plan.version += 1;
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        }
    };

    match result {
        Ok(()) => {
            finalize(
                id,
                &swarm_id,
                &req_session_id,
                "task_graph_complete",
                1,
                client_event_tx,
                swarm_members,
                swarms_by_id,
                swarm_plans,
                swarm_coordinators,
                event_history,
                event_counter,
                swarm_event_tx,
            )
            .await;
        }
        Err(e) => err(client_event_tx, id, format!("Complete rejected: {e}")),
    }
}

/// Inject gap/fix nodes from a gate the caller owns.
#[expect(
    clippy::too_many_arguments,
    reason = "swarm op threads runtime handles"
)]
pub(super) async fn handle_comm_inject_gap(
    id: u64,
    req_session_id: String,
    gate_id: String,
    nodes: Vec<TaskGraphNodeSpec>,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
    swarm_members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    swarms_by_id: &Arc<RwLock<HashMap<String, HashSet<String>>>>,
    swarm_plans: &Arc<RwLock<HashMap<String, VersionedPlan>>>,
    swarm_coordinators: &Arc<RwLock<HashMap<String, String>>>,
    event_history: &Arc<RwLock<std::collections::VecDeque<SwarmEvent>>>,
    event_counter: &Arc<std::sync::atomic::AtomicU64>,
    swarm_event_tx: &broadcast::Sender<SwarmEvent>,
    sessions: &super::SessionAgents,
    provider_template: &Arc<dyn crate::provider::Provider>,
) {
    let Some(swarm_id) = swarm_id_for(&req_session_id, swarm_members).await else {
        err(client_event_tx, id, "Not in a swarm.".to_string());
        return;
    };
    let specs: Vec<NodeSpec> = nodes.into_iter().map(spec_from_wire).collect();
    let count = specs.len();

    let Some(admitted_plan) = swarm_plans.read().await.get(&swarm_id).cloned() else {
        err(client_event_tx, id, "No plan for this swarm.".into());
        return;
    };
    let mut actor_routing = match validate_graph_actor(
        &req_session_id,
        &gate_id,
        &admitted_plan,
        sessions,
        swarm_members,
        provider_template,
    )
    .await
    {
        Ok(routing) => routing,
        Err(error) => {
            err(
                client_event_tx,
                id,
                format!("Node execution rejected: {error}"),
            );
            return;
        }
    };
    let parent = admitted_plan
        .node_meta
        .get(&gate_id)
        .cloned()
        .unwrap_or_default();
    let allowed = match validate_graph_specs(
        &req_session_id,
        &specs,
        &parent,
        false && admitted_plan.mode.eq_ignore_ascii_case("deep"),
        sessions,
        swarm_members,
        provider_template,
    )
    .await
    {
        Ok(allowed) => allowed,
        Err(error) => {
            err(
                client_event_tx,
                id,
                format!("Graph routing rejected: {error}"),
            );
            return;
        }
    };
    let result = {
        let mut plans = swarm_plans.write().await;
        let Some(plan) = plans.get_mut(&swarm_id) else {
            err(client_event_tx, id, "No plan for this swarm.".to_string());
            return;
        };
        if plan.version != admitted_plan.version {
            err(
                client_event_tx,
                id,
                "Plan changed during admission; retry graph operation.".into(),
            );
            return;
        }
        let _execution_guard = match bind_graph_actor(
            &req_session_id,
            plan.node_meta
                .get(&gate_id)
                .is_some_and(|meta| meta.resolved_execution.is_some()),
            &mut actor_routing,
            sessions,
            swarm_members,
            provider_template,
        ) {
            Ok(guard) => guard,
            Err(error) => {
                err(
                    client_event_tx,
                    id,
                    format!("Node execution rejected: {error}"),
                );
                return;
            }
        };
        let mut graph = to_task_graph(plan);
        let before = graph.clone();
        pin_graph_actor(&mut graph, &gate_id, actor_routing.as_ref());
        claim_queued_node_for_actor(&mut graph, &gate_id, &req_session_id);
        match dag::inject_from_gate(&mut graph, &gate_id, &req_session_id, specs) {
            Ok(_) => match graph_size_error(&graph) {
                Some(message) => Err(message),
                None => {
                    restrict_new_graph_nodes(&mut graph, &before, allowed.as_deref());
                    apply_task_graph(plan, &graph);
                    plan.version += 1;
                    Ok(())
                }
            },
            Err(e) => Err(e.to_string()),
        }
    };

    match result {
        Ok(()) => {
            finalize(
                id,
                &swarm_id,
                &req_session_id,
                "task_graph_inject_gap",
                count,
                client_event_tx,
                swarm_members,
                swarms_by_id,
                swarm_plans,
                swarm_coordinators,
                event_history,
                event_counter,
                swarm_event_tx,
            )
            .await;
        }
        Err(e) => err(client_event_tx, id, format!("Inject rejected: {e}")),
    }
}
