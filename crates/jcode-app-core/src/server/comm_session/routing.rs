//! Swarm route admission, policy inheritance, and initialized identity verification.
use super::{SessionAgents, resolve_swarm_spawn_effort};
use crate::protocol::SwarmRoutingStatus;
use crate::provider::{ModelRoute, Provider, RouteSelection};
use crate::server::SwarmMember;
use crate::session::Session;
use jcode_plan::ResolvedExecution;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(in crate::server) fn provider_key_for_spawn_model(
    model: Option<&str>,
    provider_key_override: Option<&str>,
) -> Option<String> {
    if let Some(provider_key) = provider_key_override
        .map(str::trim)
        .filter(|provider_key| !provider_key.is_empty())
    {
        return Some(provider_key.to_string());
    }

    let model = model?.trim();
    if model.is_empty() {
        return None;
    }

    if let Some((prefix, _rest)) = model.split_once(':') {
        let prefix = prefix.trim();
        if crate::provider::provider_from_model_key(prefix).is_some()
            || crate::provider_catalog::resolve_openai_compatible_profile_selection(prefix)
                .is_some()
            || crate::config::config().providers.contains_key(prefix)
        {
            return Some(prefix.to_string());
        }
    }

    crate::provider::provider_for_model(model).map(str::to_string)
}

/// The model/auth identity a spawned swarm agent should inherit from its
/// coordinator. Resolved with a persisted-session fallback so it stays correct
/// even when the coordinator agent is mid-turn (its mutex held), which is the
/// common case because spawns are issued from inside the coordinator's turn.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::server) struct CoordinatorSpawnIdentity {
    pub model: Option<String>,
    pub provider_key: Option<String>,
    pub route_api_method: Option<String>,
    pub is_canary: bool,
}

/// The resolved model + auth route a spawned swarm agent should be created
/// with, after reconciling `agents.swarm_model` config against the
/// coordinator's identity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::server) struct SwarmSpawnSelection {
    pub model: Option<String>,
    pub provider_key: Option<String>,
    pub route_api_method: Option<String>,
}

/// Resolve the coordinator's model/auth identity without blocking on its agent
/// mutex. During an active coordinator turn the agent lock is held for the
/// whole turn, so `try_lock` fails exactly when a spawn is issued. We fall back
/// to the persisted session snapshot so spawned agents still inherit the
/// coordinator's model, provider key, and auth route instead of silently
/// dropping to the config default (e.g. Claude OAuth instead of the API route).
pub(in crate::server) async fn resolve_coordinator_spawn_identity(
    req_session_id: &str,
    sessions: &SessionAgents,
) -> CoordinatorSpawnIdentity {
    if let Some(agent) = {
        let agent_sessions = sessions.read().await;
        agent_sessions.get(req_session_id).cloned()
    } && let Ok(agent_guard) = agent.try_lock()
    {
        if let Ok(actual) = initialized_swarm_execution(agent_guard.provider_fork().as_ref()) {
            return CoordinatorSpawnIdentity {
                model: Some(actual.model),
                provider_key: Some(actual.provider_key),
                route_api_method: Some(actual.api_method),
                is_canary: agent_guard.is_canary(),
            };
        }
        return CoordinatorSpawnIdentity {
            model: Some(agent_guard.provider_model()),
            provider_key: agent_guard.session_provider_key(),
            route_api_method: agent_guard.session_route_api_method(),
            is_canary: agent_guard.is_canary(),
        };
    }

    // Agent busy (mid-turn) or not resident: read the authoritative persisted
    // session snapshot instead of falling back to config defaults.
    match Session::load_startup_stub(req_session_id) {
        Ok(session) => {
            let identity = CoordinatorSpawnIdentity {
                model: session.model.clone(),
                provider_key: session.provider_key.clone(),
                route_api_method: session.route_api_method.clone(),
                is_canary: session.is_canary,
            };
            crate::logging::info(&format!(
                "Swarm spawn: coordinator {} agent busy/unavailable, inheriting identity from persisted session (model={:?} provider_key={:?} route={:?} canary={})",
                req_session_id,
                identity.model,
                identity.provider_key,
                identity.route_api_method,
                identity.is_canary,
            ));
            identity
        }
        Err(error) => {
            crate::logging::warn(&format!(
                "Swarm spawn: failed to load persisted coordinator session {} for model inheritance: {} (spawned agent will use server defaults)",
                req_session_id, error
            ));
            CoordinatorSpawnIdentity::default()
        }
    }
}

/// Split a configured swarm model that carries an explicit auth-route prefix
/// (`openai-api:`, `openai-oauth:`, `claude-api:`, `claude-oauth:`) into a
/// structured selection so spawned sessions pin the exact provider + auth
/// method instead of guessing from the bare model name.
///
/// Example: `agents.swarm_model = "openai-api:gpt-5.5"` resolves to
/// `model = gpt-5.5`, `provider_key = openai-api-key`,
/// `route_api_method = openai-api-key`, which makes every spawned agent use
/// GPT-5.5 on the OpenAI API key route regardless of the coordinator's model.
///
/// Returns `None` for models without such a prefix, or for prefixes that carry
/// no API-vs-OAuth decision (bare provider aliases, OpenRouter, Copilot, ...).
/// Those keep their prefixed model and route correctly via the existing
/// session-restore path.
fn explicit_route_for_configured_model(model: &str) -> Option<SwarmSpawnSelection> {
    let (_, prefix, bare) = crate::provider::explicit_model_provider_prefix(model)?;
    let bare = bare.trim();
    if bare.is_empty() {
        return None;
    }
    // Only the dual-auth (Anthropic/OpenAI OAuth-vs-API) prefixes carry an
    // explicit credential decision worth pinning. The canonical parser maps the
    // prefix to its stable route id, which `ModelRouteApiMethod::parse` round-
    // trips back to the exact auth method when the spawned session is restored.
    let route_id = jcode_provider_core::AuthRoute::parse_explicit_credential_prefix(prefix)?
        .route_api_method();
    Some(SwarmSpawnSelection {
        model: Some(bare.to_string()),
        provider_key: Some(route_id.to_string()),
        route_api_method: Some(route_id.to_string()),
    })
}

/// True when a model string is one of the "inherit the coordinator" sentinels.
fn is_inherit_sentinel(model: &str) -> bool {
    let trimmed = model.trim();
    trimmed.eq_ignore_ascii_case("inherit") || trimmed.eq_ignore_ascii_case("coordinator")
}

/// Selection that inherits the coordinator's model, provider key, and route.
fn inherit_coordinator_selection(coordinator: &CoordinatorSpawnIdentity) -> SwarmSpawnSelection {
    SwarmSpawnSelection {
        model: coordinator.model.clone(),
        provider_key: coordinator
            .provider_key
            .clone()
            .or_else(|| provider_key_for_spawn_model(coordinator.model.as_deref(), None)),
        route_api_method: coordinator.route_api_method.clone(),
    }
}

/// Selection for a concrete model string (optionally route-prefixed like
/// `openai-api:gpt-5.5`), reconciled against the coordinator's identity.
fn selection_for_concrete_model(
    model: String,
    coordinator: &CoordinatorSpawnIdentity,
) -> SwarmSpawnSelection {
    // A model may pin an explicit provider + auth route via a prefix
    // (e.g. "openai-api:gpt-5.5"). Honor it directly so spawned agents do
    // NOT inherit the coordinator's model/auth and instead use the
    // requested model on the requested API route.
    if let Some(selection) = explicit_route_for_configured_model(&model) {
        return selection;
    }

    // A concrete model only inherits the coordinator's provider_key/route
    // when it targets the same model; otherwise the route would point at
    // the wrong provider/auth mode.
    if coordinator.model.as_deref() == Some(model.as_str()) {
        SwarmSpawnSelection {
            model: Some(model.clone()),
            provider_key: coordinator
                .provider_key
                .clone()
                .or_else(|| provider_key_for_spawn_model(Some(&model), None)),
            route_api_method: coordinator.route_api_method.clone(),
        }
    } else {
        SwarmSpawnSelection {
            provider_key: provider_key_for_spawn_model(Some(&model), None),
            model: Some(model),
            route_api_method: None,
        }
    }
}

pub(super) fn resolve_swarm_spawn_selection(
    requested_model: Option<String>,
    configured_swarm_model: Option<String>,
    coordinator: &CoordinatorSpawnIdentity,
) -> SwarmSpawnSelection {
    // An explicit per-worker choice overrides the configured default. The
    // inheritance sentinels bypass even a concrete configured model.
    if let Some(model) = requested_model
        .map(|model| model.trim().to_string())
        .filter(|model| !model.is_empty())
    {
        return if is_inherit_sentinel(&model) {
            inherit_coordinator_selection(coordinator)
        } else {
            selection_for_concrete_model(model, coordinator)
        };
    }
    // Treat empty strings and the explicit "inherit"/"coordinator" sentinels as
    // "no override": spawned swarm agents should inherit the coordinator's model
    // unless `agents.swarm_model` is deliberately set to a concrete model. This
    // avoids the surprising case where a stale `swarm_model` config pins every
    // spawned agent to an unrelated model/provider.
    let configured_swarm_model = configured_swarm_model
        .map(|model| model.trim().to_string())
        .filter(|model| !model.trim().is_empty() && !is_inherit_sentinel(model));

    match configured_swarm_model {
        Some(model) => selection_for_concrete_model(model, coordinator),
        None => inherit_coordinator_selection(coordinator),
    }
}

// Keep model aliases separate from route identity. In particular, never strip
// arbitrary suffixes or use prefix matching (gpt-X is not gpt-X-pro).
fn swarm_model_key(model: &str) -> String {
    crate::provider::normalize_copilot_model_name(model.trim())
        .unwrap_or(model.trim())
        .to_ascii_lowercase()
}

pub(in crate::server) fn same_swarm_execution(
    a: &ResolvedExecution,
    b: &ResolvedExecution,
) -> bool {
    swarm_model_key(&a.model) == swarm_model_key(&b.model)
        && a.provider_key == b.provider_key
        && a.api_method == b.api_method
        && a.provider_label == b.provider_label
}

pub(super) fn execution_for_route(route: &ModelRoute, effort: Option<String>) -> ResolvedExecution {
    let selection = RouteSelection::from_model_route(route);
    ResolvedExecution {
        model: selection.model,
        provider_key: selection.runtime_key.stable_id(),
        api_method: selection.api_method,
        provider_label: selection.provider_label,
        effort,
    }
}

/// Observe initialized provider state, not session fields copied from a request.
/// An opaque/ambiguous runtime has no verified identity and must fail closed.
pub(in crate::server) fn initialized_swarm_execution(
    provider: &dyn Provider,
) -> anyhow::Result<ResolvedExecution> {
    use jcode_provider_core::{ResolvedCredential, RuntimeKey};
    let model = provider.model();
    let compatible = provider.direct_openai_compatible_route_parts();
    let credential = provider.active_resolved_credential();
    let pin = provider.explicit_provider_pin_for_current_model();
    let routes = provider.model_routes();
    let candidates: Vec<_> = routes
        .iter()
        .filter(|route| {
            let selection = RouteSelection::from_model_route(route);
            let actual_model = if let Some(pin) = pin.as_deref().filter(|_| !model.contains('@')) {
                format!("{model}@{pin}")
            } else {
                model.clone()
            };
            if swarm_model_key(&route.model) != swarm_model_key(&actual_model) {
                return false;
            }
            match &selection.runtime_key {
                RuntimeKey::OpenAiCompatible { .. } => {
                    compatible.as_ref().is_some_and(|(label, api, _)| {
                        label == &route.provider && api == &route.api_method
                    })
                }
                RuntimeKey::OpenRouter => provider.supports_provider_routing_features(),
                RuntimeKey::ClaudeOAuth | RuntimeKey::OpenAIOAuth => {
                    credential == Some(ResolvedCredential::Oauth)
                        && jcode_provider_core::model_route_provider_labels_match(
                            &route.provider,
                            provider.name(),
                        )
                }
                RuntimeKey::AnthropicApiKey | RuntimeKey::OpenAIApiKey => {
                    credential == Some(ResolvedCredential::ApiKey)
                        && jcode_provider_core::model_route_provider_labels_match(
                            &route.provider,
                            provider.name(),
                        )
                }
                RuntimeKey::Current | RuntimeKey::RemoteCatalog | RuntimeKey::Other(_) => false,
                _ => {
                    compatible.is_none()
                        && jcode_provider_core::model_route_provider_labels_match(
                            &route.provider,
                            &provider.runtime_display_name(),
                        )
                }
            }
        })
        .collect();
    let Some(first) = candidates.first() else {
        anyhow::bail!("Initialized provider identity cannot be verified against its catalog.");
    };
    let actual = execution_for_route(first, provider.reasoning_effort());
    if candidates
        .iter()
        .skip(1)
        .any(|r| !same_swarm_execution(&actual, &execution_for_route(r, None)))
    {
        anyhow::bail!("Initialized provider identity is ambiguous.");
    }
    Ok(actual)
}

pub(in crate::server) fn verify_swarm_execution(
    provider: &dyn Provider,
    expected: &ResolvedExecution,
) -> anyhow::Result<ResolvedExecution> {
    let actual = initialized_swarm_execution(provider)?;
    if !same_swarm_execution(&actual, expected) {
        anyhow::bail!(
            "Initialized swarm route differs from the admitted model/provider/API route; refusing fallback."
        );
    }
    if actual.effort != expected.effort {
        anyhow::bail!(
            "Initialized swarm effort {:?} does not match pinned effort {:?}.",
            actual.effort,
            expected.effort
        );
    }
    Ok(actual)
}

pub(in crate::server) fn initialize_swarm_provider(
    provider: &dyn Provider,
    target: &ResolvedExecution,
    enforce_effort: bool,
) -> anyhow::Result<ResolvedExecution> {
    let route = provider
        .model_routes()
        .into_iter()
        .find(|r| r.available && same_swarm_execution(&execution_for_route(r, None), target))
        .ok_or_else(|| {
            anyhow::anyhow!("Admitted swarm route became unavailable before initialization.")
        })?;
    provider.set_route_selection(&RouteSelection::from_model_route(&route))?;
    if let Some(effort) = target.effort.as_deref()
        && provider.reasoning_effort().as_deref() != Some(effort)
    {
        if let Err(error) = provider.set_reasoning_effort(effort) {
            if enforce_effort {
                return Err(error);
            }
            crate::logging::warn(&format!(
                "Configured swarm effort could not be applied: {error}"
            ));
        }
    }
    // Only first admission with omitted caller effort may tolerate a configured
    // default the provider cannot apply. Pin the observed effort, including None,
    // so subsequent initialization never reinterprets it using new defaults.
    let mut effective = target.clone();
    if !enforce_effort {
        effective.effort = provider.reasoning_effort();
    }
    verify_swarm_execution(provider, &effective)
}

fn selector_matches_route(selector: &str, route: &ModelRoute) -> bool {
    let selector = selector.trim();
    let selection = RouteSelection::from_model_route(route);
    if swarm_model_key(selector) == swarm_model_key(&selection.routed_model_spec()) {
        return true;
    }
    if let Some((_, prefix, model)) = crate::provider::explicit_model_provider_prefix(selector) {
        return swarm_model_key(model) == swarm_model_key(&route.model)
            && jcode_provider_core::model_route_provider_matches_key(
                Some(&selection.runtime_key.stable_id()),
                &route.provider,
                prefix.trim_end_matches(':'),
            );
    }
    // Compatible profiles have catalog-defined prefixes rather than a second
    // static provider table. Compare the existing route's rendered selector.
    if let Some((prefix, model)) = selector.split_once(':') {
        if let jcode_provider_core::RuntimeKey::OpenAiCompatible {
            profile_id: Some(profile),
        } = &selection.runtime_key
        {
            return prefix == profile && swarm_model_key(model) == swarm_model_key(&route.model);
        }
        return false;
    }
    swarm_model_key(selector) == swarm_model_key(&route.model)
}

pub(in crate::server) fn permitted_route(route: &ModelRoute, allowed: Option<&[String]>) -> bool {
    allowed.is_none_or(|allowed| allowed.iter().any(|s| selector_matches_route(s, route)))
}

/// Resolve admission against the real catalog, without mutating a provider or
/// claiming a task. Disabled no-override behavior retains the legacy defaults.
pub(in crate::server) fn admit_swarm_spawn_selection(
    requested_model: Option<String>,
    configured_swarm_model: Option<String>,
    coordinator: &CoordinatorSpawnIdentity,
    enabled: bool,
    allowed: Option<&[String]>,
    routes: &[ModelRoute],
) -> anyhow::Result<SwarmSpawnSelection> {
    if requested_model.is_some() && !enabled {
        anyhow::bail!(
            "Explicit swarm model selection is disabled. Enable agents.swarm_model_selection or omit model (including inherit/coordinator)."
        );
    }
    if requested_model
        .as_deref()
        .is_some_and(|s| s.trim().is_empty())
    {
        anyhow::bail!("model must not be blank; omit model to use the default.");
    }
    let legacy = resolve_swarm_spawn_selection(
        requested_model.clone(),
        configured_swarm_model.clone(),
        coordinator,
    );
    if !enabled && allowed.is_none() {
        return Ok(legacy);
    }
    let selector = requested_model
        .as_deref()
        .or(configured_swarm_model.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let inherit = selector.is_none_or(is_inherit_sentinel);
    let mut matches: Vec<&ModelRoute> =
        routes
            .iter()
            .filter(|route| {
                if inherit {
                    let selected = RouteSelection::from_model_route(route);
                    if let Some(auth) = coordinator
                        .provider_key
                        .as_deref()
                        .and_then(jcode_provider_core::AuthRoute::parse_explicit_credential_prefix)
                    {
                        if jcode_provider_core::ModelRouteApiMethod::parse(auth.route_api_method())
                            != route.api_method_kind()
                        {
                            return false;
                        }
                    }

                    coordinator.model.as_deref().is_some_and(|model| {
                        swarm_model_key(model) == swarm_model_key(&route.model)
                    }) && coordinator
                        .route_api_method
                        .as_deref()
                        .map(|api| {
                            jcode_provider_core::ModelRouteApiMethod::parse(api)
                                == route.api_method_kind()
                        })
                        .or_else(|| {
                            coordinator.provider_key.as_deref().map(|key| {
                                jcode_provider_core::model_route_provider_matches_key(
                                    Some(&selected.runtime_key.stable_id()),
                                    &route.provider,
                                    key,
                                )
                            })
                        })
                        .unwrap_or(false)
                } else {
                    selector.is_some_and(|selector| selector_matches_route(selector, route))
                }
            })
            .filter(|route| route.available)
            .collect();
    matches.sort_by_key(|route| RouteSelection::from_model_route(route).routed_model_spec());
    matches.dedup_by(|a, b| {
        same_swarm_execution(&execution_for_route(a, None), &execution_for_route(b, None))
    });
    if matches.is_empty() {
        anyhow::bail!(
            "No available route for swarm model {:?}. Authenticate the selected provider or choose an available list_models route; no fallback was used.",
            selector.or(coordinator.model.as_deref())
        );
    }
    if matches.len() > 1 {
        anyhow::bail!(
            "Ambiguous swarm model. Select a concrete route: {}",
            matches
                .iter()
                .map(|route| RouteSelection::from_model_route(route).routed_model_spec())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let route = matches[0];
    if !permitted_route(route, allowed) {
        anyhow::bail!(
            "Swarm route '{}' is prohibited by the current operator/subtree allowed_models policy.",
            RouteSelection::from_model_route(route).routed_model_spec()
        );
    }
    let execution = execution_for_route(route, None);
    Ok(SwarmSpawnSelection {
        model: Some(execution.model),
        provider_key: Some(execution.provider_key),
        route_api_method: Some(execution.api_method),
    })
}

/// Canonicalize restrictions to concrete catalog selectors when intersecting.
/// Include unavailable entries so a temporarily unavailable route cannot hide
/// a broadening request and become authorized after authentication changes.
pub(in crate::server) fn narrow_swarm_allowed_models(
    inherited: Option<&[String]>,
    requested: Option<&[String]>,
    routes: &[ModelRoute],
) -> anyhow::Result<Option<Vec<String>>> {
    let Some(requested) = requested else {
        return Ok(inherited.map(<[String]>::to_vec));
    };
    for selector in requested {
        let matching: Vec<_> = routes
            .iter()
            .filter(|r| selector_matches_route(selector, r))
            .collect();
        if matching.is_empty() {
            anyhow::bail!(
                "Unknown allowed_models selector '{selector}'; choose a list_models route."
            );
        }
        if matching
            .iter()
            .any(|route| !permitted_route(route, inherited))
        {
            anyhow::bail!(
                "allowed_models may narrow but cannot broaden inherited routing restrictions: '{selector}'."
            );
        }
    }
    let mut narrowed: Vec<_> = routes
        .iter()
        .filter(|r| permitted_route(r, Some(requested)))
        .map(|r| RouteSelection::from_model_route(r).routed_model_spec())
        .collect();
    narrowed.retain(|selector| {
        routes
            .iter()
            .filter(|r| selector_matches_route(selector, r))
            .all(|r| permitted_route(r, Some(requested)) && permitted_route(r, inherited))
    });
    narrowed.sort();
    narrowed.dedup();
    Ok(Some(narrowed))
}

/// Current operator policy is intersected with every trusted durable ancestor.
/// Missing ancestry is an error, not an unrestricted root.
pub(in crate::server) fn effective_swarm_allowed_models(
    members: &HashMap<String, SwarmMember>,
    session_id: &str,
    operator: Option<&[String]>,
    requested: Option<&[String]>,
    routes: &[ModelRoute],
) -> anyhow::Result<Option<Vec<String>>> {
    let ancestry = crate::server::swarm::trusted_swarm_ancestry(members, session_id)?;
    let restrictions: Vec<&[String]> = ancestry
        .iter()
        .filter_map(|id| members[id].allowed_models.as_deref())
        .chain(operator)
        .collect();
    let inherited = if restrictions.is_empty() {
        None
    } else {
        let mut selectors: Vec<_> = routes
            .iter()
            .filter(|route| {
                restrictions
                    .iter()
                    .all(|allowed| permitted_route(route, Some(allowed)))
            })
            .map(|route| RouteSelection::from_model_route(route).routed_model_spec())
            .collect();
        selectors.retain(|selector| {
            routes
                .iter()
                .filter(|r| selector_matches_route(selector, r))
                .all(|r| {
                    restrictions
                        .iter()
                        .all(|allowed| permitted_route(r, Some(allowed)))
                })
        });
        selectors.sort();
        selectors.dedup();
        Some(selectors)
    };
    narrow_swarm_allowed_models(inherited.as_deref(), requested, routes)
}

/// Shared preclaim admission for direct spawn and the DAG scheduler. A pinned
/// target is revalidated, never rerouted to a new default on retry/resume.
pub(in crate::server) async fn admit_swarm_execution(
    session_id: &str,
    requested_model: Option<String>,
    requested_effort: Option<String>,
    requested_allowed: Option<&[String]>,
    pinned: Option<&ResolvedExecution>,
    sessions: &SessionAgents,
    members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    provider: &Arc<dyn Provider>,
) -> anyhow::Result<SwarmRoutingStatus> {
    if requested_model
        .as_deref()
        .is_some_and(|s| s.trim().is_empty())
    {
        anyhow::bail!("model must not be blank; omit model to use the default.");
    }
    if requested_effort
        .as_deref()
        .is_some_and(|s| s.trim().is_empty())
    {
        anyhow::bail!("effort must not be blank; omit effort to use the default.");
    }
    let config = &crate::config::config().agents;
    if requested_model.is_some() && !config.swarm_model_selection {
        anyhow::bail!(
            "Explicit swarm model selection is disabled. Enable agents.swarm_model_selection or omit model."
        );
    }
    let routes = provider.model_routes();
    let members = members.read().await;
    let ancestry = crate::server::swarm::trusted_swarm_ancestry(&members, session_id)?;
    match ancestry.as_slice() {
        [] => anyhow::bail!("Trusted swarm ancestry is empty; refusing spawn admission."),
        [_, .., root]
            if !crate::session_effort::session_effort(root)
                .as_deref()
                .is_some_and(crate::prompt::is_deep_swarm_effort) =>
        {
            anyhow::bail!(
                "Recursive swarm spawning is disabled for normal/light swarms. The trusted root must run in swarm-deep mode."
            );
        }
        _ => {}
    }
    let allowed = effective_swarm_allowed_models(
        &members,
        session_id,
        config
            .swarm_model_selection
            .then_some(config.swarm_allowed_models.as_deref())
            .flatten(),
        requested_allowed,
        &routes,
    )?;
    drop(members);
    let coordinator = resolve_coordinator_spawn_identity(session_id, sessions).await;
    let effort =
        resolve_swarm_spawn_effort(requested_effort.as_deref(), config.swarm_effort.as_deref());
    let mut resolved = if let Some(pinned) = pinned {
        let route = routes
            .iter()
            .find(|r| r.available && same_swarm_execution(&execution_for_route(r, None), pinned))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Pinned swarm route is unavailable; refusing to replace it with a fallback."
                )
            })?;
        if requested_model.as_deref().is_some_and(|selector| {
            !is_inherit_sentinel(selector) && !selector_matches_route(selector, route)
        }) {
            anyhow::bail!("Requested model conflicts with pinned swarm execution.");
        }
        if !permitted_route(route, allowed.as_deref()) {
            anyhow::bail!("Pinned swarm route is prohibited by current routing policy.");
        }
        if requested_effort
            .as_deref()
            .is_some_and(|e| pinned.effort.as_deref() != Some(e.trim()))
        {
            anyhow::bail!("Requested effort conflicts with pinned swarm execution.");
        }
        Some(pinned.clone())
    } else {
        let selection = admit_swarm_spawn_selection(
            requested_model.clone(),
            config.swarm_model.clone(),
            &coordinator,
            config.swarm_model_selection,
            allowed.as_deref(),
            &routes,
        )?;
        routes
            .iter()
            .find(|r| {
                selection.model.as_deref() == Some(r.model.as_str())
                    && selection.provider_key.as_deref()
                        == Some(
                            RouteSelection::from_model_route(r)
                                .runtime_key
                                .stable_id()
                                .as_str(),
                        )
                    && selection.route_api_method.as_deref() == Some(r.api_method.as_str())
            })
            .map(|r| execution_for_route(r, effort))
    };
    if let Some(target) = resolved.as_mut() {
        let probe = provider.fork();
        *target = initialize_swarm_provider(
            probe.as_ref(),
            target,
            pinned.is_some() || requested_effort.is_some(),
        )?;
    }
    Ok(SwarmRoutingStatus {
        requested_model,
        requested_effort,
        resolved_execution: resolved,
        actual_execution: None,
        selection_enabled: Some(config.swarm_model_selection),
        allowed_models: allowed,
    })
}

pub(in crate::server) async fn verify_swarm_session_startup(
    session_id: &str,
    sessions: &SessionAgents,
    members: &Arc<RwLock<HashMap<String, SwarmMember>>>,
    provider_template: &Arc<dyn Provider>,
) -> anyhow::Result<()> {
    let Some(mut routing) = members
        .read()
        .await
        .get(session_id)
        .and_then(|m| m.runtime.routing.clone())
    else {
        return Ok(());
    };
    let agent = sessions
        .read()
        .await
        .get(session_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Prepared swarm session did not initialize."))?;
    let provider = match agent.try_lock() {
        Ok(agent) => agent.provider_fork(),
        Err(_) if routing.actual_execution.is_some() => return Ok(()),
        Err(_) => anyhow::bail!("Restored swarm session is busy before identity verification."),
    };
    // Running sessions are not revoked by a later policy change. A restored
    // session has no actual_execution and must pass current policy again.
    if routing.actual_execution.is_none() {
        let config = &crate::config::config().agents;
        if routing.requested_model.is_some() && !config.swarm_model_selection {
            anyhow::bail!("Restored explicit routing requires agents.swarm_model_selection.");
        }
        let routes = provider_template.model_routes();
        routing.allowed_models = effective_swarm_allowed_models(
            &*members.read().await,
            session_id,
            config
                .swarm_model_selection
                .then_some(config.swarm_allowed_models.as_deref())
                .flatten(),
            None,
            &routes,
        )?;
        if let Some(target) = routing.resolved_execution.as_ref() {
            if !routes.iter().any(|route| {
                route.available
                    && same_swarm_execution(&execution_for_route(route, None), target)
                    && permitted_route(route, routing.allowed_models.as_deref())
            }) {
                anyhow::bail!(
                    "Restored swarm execution is unavailable or prohibited by current subtree policy."
                );
            }
        }
        routing.selection_enabled = Some(config.swarm_model_selection);
    }
    routing.actual_execution = match routing.resolved_execution.as_ref() {
        Some(target) => Some(verify_swarm_execution(provider.as_ref(), target)?),
        None => {
            if routing
                .requested_effort
                .as_deref()
                .is_some_and(|e| provider.reasoning_effort().as_deref() != Some(e.trim()))
            {
                anyhow::bail!("Restored worker did not apply explicit effort.");
            }
            observe_optional_swarm_execution(provider.as_ref())
        }
    };
    if let Some(member) = members.write().await.get_mut(session_id) {
        member.runtime.routing = Some(routing);
    }
    Ok(())
}

/// Legacy providers without a catalog may run without a concrete route pin.
/// Never manufacture a verified identity when their runtime cannot report one.
pub(in crate::server) fn observe_optional_swarm_execution(
    provider: &dyn Provider,
) -> Option<ResolvedExecution> {
    match initialized_swarm_execution(provider) {
        Ok(actual) => Some(actual),
        Err(error) => {
            crate::logging::debug(&format!("Swarm runtime identity unavailable: {error}"));
            None
        }
    }
}

pub(super) fn spawn_routing_fingerprints(
    model: &Option<String>,
    effort: &Option<String>,
    allowed_models: &Option<Vec<String>>,
) -> anyhow::Result<(String, String)> {
    use anyhow::Context;
    Ok((
        serde_json::to_string(allowed_models)
            .context("Could not encode spawn allowed-model policy")?,
        serde_json::to_string(&(model, effort, allowed_models))
            .context("Could not encode spawn routing request")?,
    ))
}
