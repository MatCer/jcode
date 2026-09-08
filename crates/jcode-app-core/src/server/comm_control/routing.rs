// Node routing admission and fresh execution verification.
use super::*;

pub(super) fn node_requested_effort(
    meta: &NodeMeta,
    requested: Option<String>,
) -> anyhow::Result<Option<String>> {
    let requested = requested.map(|value| value.trim().to_string());
    if requested.as_deref() == Some("") {
        anyhow::bail!("effort must not be blank; omit effort to use the node setting.");
    }
    if let (Some(node), Some(requested)) = (&meta.effort, &requested)
        && node.trim() != requested
    {
        anyhow::bail!("Assignment effort conflicts with the node's authoritative effort.");
    }
    Ok(meta.effort.clone().or(requested))
}

pub(super) fn execution_matches_node(
    expected: &ResolvedExecution,
    actual: &ResolvedExecution,
    explicit_effort: Option<&str>,
    pinned: bool,
) -> bool {
    crate::server::comm_session::same_swarm_execution(expected, actual)
        && explicit_effort.is_none_or(|effort| actual.effort.as_deref() == Some(effort.trim()))
        && (!pinned || expected.effort == actual.effort)
}

pub(super) fn node_routing_requirement(routing: &SwarmRoutingStatus) -> String {
    match routing.resolved_execution.as_ref() {
        Some(route) => format!(
            "model={}, provider={}, API={}, effort={}",
            route.model,
            route.provider_key,
            route.api_method,
            route
                .effort
                .as_deref()
                .or(routing.requested_effort.as_deref())
                .unwrap_or("inherited")
        ),
        None => format!(
            "legacy inherited route, effort={}",
            routing.requested_effort.as_deref().unwrap_or("inherited")
        ),
    }
}

/// Intersect durable restrictions, rather than treating an old snapshot as a
/// new request to broaden today's policy. Use the existing catalog predicate.
pub(in crate::server) fn intersect_node_allowed_models(
    current: Option<&[String]>,
    snapshot: Option<&[String]>,
    routes: &[crate::provider::ModelRoute],
) -> Option<Vec<String>> {
    use crate::server::comm_session::permitted_route;
    use jcode_provider_core::RouteSelection;
    if current.is_none() && snapshot.is_none() {
        return None;
    }
    let mut allowed: Vec<String> = routes
        .iter()
        .filter(|route| permitted_route(route, current) && permitted_route(route, snapshot))
        .map(|route| RouteSelection::from_model_route(route).routed_model_spec())
        .collect();
    // A selector can project several API routes. Never widen a restriction
    // through such a lossy projection.
    allowed.retain(|selector| {
        routes
            .iter()
            .filter(|route| permitted_route(route, Some(std::slice::from_ref(selector))))
            .all(|route| permitted_route(route, current) && permitted_route(route, snapshot))
    });
    allowed.sort();
    allowed.dedup();
    Some(allowed)
}

#[expect(
    clippy::too_many_arguments,
    reason = "node admission shares the spawn policy and trusted runtime handles"
)]
pub(in crate::server) async fn admit_node_execution(
    requester: &str,
    meta: &NodeMeta,
    model: Option<String>,
    effort: Option<String>,
    allowed_models: Option<&[String]>,
    sessions: &SessionAgents,
    members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    provider: &Arc<dyn crate::provider::Provider>,
) -> anyhow::Result<SwarmRoutingStatus> {
    use crate::server::comm_session::{
        admit_swarm_execution, effective_swarm_allowed_models, same_swarm_execution,
    };
    let effort = node_requested_effort(meta, effort)?;
    let config = &crate::config::config().agents;
    let routes = provider.model_routes();
    let current = effective_swarm_allowed_models(
        &*members.read().await,
        requester,
        config
            .swarm_model_selection
            .then_some(config.swarm_allowed_models.as_deref())
            .flatten(),
        allowed_models,
        &routes,
    )?;
    let allowed =
        intersect_node_allowed_models(current.as_deref(), meta.allowed_models.as_deref(), &routes);
    let mut routing = admit_swarm_execution(
        requester,
        crate::server::comm_session::SwarmExecutionRequest {
            requested_model: meta.model.clone().or(model.clone()),
            requested_effort: effort.clone(),
            requested_allowed: allowed.as_deref(),
            pinned: meta.resolved_execution.as_ref(),
        },
        sessions,
        members,
        provider,
    )
    .await?;
    if let Some(requested) = model
        && (meta.model.is_some() || meta.resolved_execution.is_some())
    {
        let override_route = admit_swarm_execution(
            requester,
            crate::server::comm_session::SwarmExecutionRequest {
                requested_model: Some(requested),
                requested_effort: effort,
                requested_allowed: allowed.as_deref(),
                pinned: None,
            },
            sessions,
            members,
            provider,
        )
        .await?;
        if !routing
            .resolved_execution
            .as_ref()
            .zip(override_route.resolved_execution.as_ref())
            .is_some_and(|(node, requested)| same_swarm_execution(node, requested))
        {
            anyhow::bail!(
                "Assignment model conflicts with the node's authoritative model/pinned route."
            );
        }
    }
    routing.allowed_models = allowed;
    Ok(routing)
}

/// Revalidate while the caller holds the agent and member locks through the
/// claim/dispatch commit. Candidate snapshots are only selection hints.
pub(in crate::server) fn observe_node_execution(
    requester: &str,
    worker: &str,
    routing: &SwarmRoutingStatus,
    pinned: bool,
    agent: &Agent,
    members: &HashMap<String, SwarmMember>,
    provider: &dyn crate::provider::Provider,
) -> anyhow::Result<SwarmRoutingStatus> {
    let actual_provider = agent.provider_fork();
    let actual =
        match crate::server::comm_session::initialized_swarm_execution(actual_provider.as_ref()) {
            Ok(actual) => Some(actual),
            Err(error)
                if routing.resolved_execution.is_some() || routing.allowed_models.is_some() =>
            {
                return Err(error.context(format!(
                    "Cannot verify worker '{worker}' initialized execution"
                )));
            }
            Err(error) => {
                // Opaque legacy providers remain eligible only without a concrete
                // target or restrictions. Policy checks below still fail closed.
                crate::logging::debug(&format!(
                    "Worker '{worker}' has opaque inherited execution: {error}"
                ));
                None
            }
        };
    let matches = match routing.resolved_execution.as_ref() {
        Some(expected) => actual.as_ref().is_some_and(|actual| {
            execution_matches_node(
                expected,
                actual,
                routing.requested_effort.as_deref(),
                pinned,
            )
        }),
        None => routing.requested_effort.as_deref().is_none_or(|effort| {
            actual_provider.reasoning_effort().as_deref() == Some(effort.trim())
        }),
    };
    if !matches {
        anyhow::bail!(
            "Worker '{worker}' routing changed or has incompatible initialized model/provider/API/effort."
        );
    }
    let config = &crate::config::config().agents;
    if routing.requested_model.is_some() && !config.swarm_model_selection {
        anyhow::bail!("Explicit swarm model selection is disabled by current routing policy.");
    }
    let routes = provider.model_routes();
    let mut allowed = routing.allowed_models.clone();
    for session in [requester, worker] {
        let current = crate::server::comm_session::effective_swarm_allowed_models(
            members,
            session,
            config
                .swarm_model_selection
                .then_some(config.swarm_allowed_models.as_deref())
                .flatten(),
            None,
            &routes,
        )?;
        allowed = intersect_node_allowed_models(allowed.as_deref(), current.as_deref(), &routes);
    }
    if (allowed.is_some() || routing.resolved_execution.is_some())
        && !actual.as_ref().is_some_and(|actual| {
            routes.iter().any(|route| {
                let selection = jcode_provider_core::RouteSelection::from_model_route(route);
                route.available
                    && selection.runtime_key.stable_id() == actual.provider_key
                    && selection.api_method == actual.api_method
                    && selection.model == actual.model
                    && crate::server::comm_session::permitted_route(route, allowed.as_deref())
            })
        })
    {
        anyhow::bail!(
            "Worker '{worker}' execution is unavailable or prohibited by current routing policy."
        );
    }
    let mut verified = routing.clone();
    verified.allowed_models = allowed;
    verified.actual_execution = actual;
    Ok(verified)
}

/// Observe the worker's initialized provider, not its requested model or label.
/// Opaque legacy providers remain usable only for unconstrained legacy nodes.
pub(in crate::server) async fn compatible_node_candidates(
    routing: &SwarmRoutingStatus,
    pinned: bool,
    sessions: &SessionAgents,
    members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    provider: &Arc<dyn crate::provider::Provider>,
) -> HashMap<String, Option<ResolvedExecution>> {
    let agents = sessions.read().await.clone();
    let member_snapshot = members.read().await.clone();
    let mut compatible = HashMap::new();
    for (session_id, agent) in agents {
        // A locked provider can have changed since the last status snapshot.
        // Cached actual_execution is never evidence for a new claim.
        let Ok(agent) = agent.try_lock() else {
            continue;
        };
        if let Ok(verified) = observe_node_execution(
            &session_id,
            &session_id,
            routing,
            pinned,
            &agent,
            &member_snapshot,
            provider.as_ref(),
        ) {
            compatible.insert(session_id, verified.actual_execution);
        }
    }
    compatible
}

pub(super) fn pin_node_execution(meta: &mut NodeMeta, routing: &SwarmRoutingStatus) {
    meta.model = routing.requested_model.clone();
    meta.effort = routing.requested_effort.clone();
    meta.allowed_models = routing.allowed_models.clone();
    if meta.resolved_execution.is_none() {
        meta.resolved_execution = routing.actual_execution.clone();
    }
}
