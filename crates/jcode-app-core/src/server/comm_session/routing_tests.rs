//! Route selection, catalog admission, ancestry policy, and runtime identity.
use super::*;

fn coordinator_identity(
    model: Option<&str>,
    provider_key: Option<&str>,
    route_api_method: Option<&str>,
) -> CoordinatorSpawnIdentity {
    CoordinatorSpawnIdentity {
        model: model.map(str::to_string),
        provider_key: provider_key.map(str::to_string),
        route_api_method: route_api_method.map(str::to_string),
        is_canary: false,
    }
}

#[test]
fn resolve_swarm_spawn_model_prefers_configured_model_over_coordinator_model() {
    let selection = resolve_swarm_spawn_selection(
        None,
        Some("openai/gpt-5.4@OpenAI".to_string()),
        &coordinator_identity(
            Some("nvidia/llama-3.3-nemotron-super-49b-v1"),
            Some("nvidia"),
            Some("openai-compatible:nvidia-nim"),
        ),
    );

    assert_eq!(selection.model.as_deref(), Some("openai/gpt-5.4@OpenAI"));
    assert_eq!(selection.provider_key.as_deref(), Some("openrouter"));
    // A different configured model must not inherit the coordinator's route.
    assert_eq!(selection.route_api_method, None);
}

#[test]
fn resolve_swarm_spawn_model_inherits_coordinator_when_unconfigured() {
    let selection = resolve_swarm_spawn_selection(
        None,
        None,
        &coordinator_identity(
            Some("nvidia/llama-3.3-nemotron-super-49b-v1"),
            Some("nvidia"),
            Some("openai-compatible:nvidia-nim"),
        ),
    );

    assert_eq!(
        selection.model.as_deref(),
        Some("nvidia/llama-3.3-nemotron-super-49b-v1")
    );
    assert_eq!(selection.provider_key.as_deref(), Some("nvidia"));
    assert_eq!(
        selection.route_api_method.as_deref(),
        Some("openai-compatible:nvidia-nim")
    );
}

#[test]
fn resolve_swarm_spawn_model_inherits_coordinator_auth_route_for_oauth_vs_api() {
    // Regression: a coordinator on the Claude API route must spawn agents on
    // the same API route, not Claude OAuth (the config default).
    let selection = resolve_swarm_spawn_selection(
        None,
        None,
        &coordinator_identity(
            Some("claude-opus-4-6"),
            Some("claude-api"),
            Some("claude-api"),
        ),
    );

    assert_eq!(selection.model.as_deref(), Some("claude-opus-4-6"));
    assert_eq!(selection.provider_key.as_deref(), Some("claude-api"));
    assert_eq!(selection.route_api_method.as_deref(), Some("claude-api"));
}

#[test]
fn resolve_swarm_spawn_model_keeps_provider_key_when_config_matches_coordinator() {
    let selection = resolve_swarm_spawn_selection(
        None,
        Some("custom-model".to_string()),
        &coordinator_identity(
            Some("custom-model"),
            Some("custom-provider"),
            Some("custom-route"),
        ),
    );

    assert_eq!(selection.model.as_deref(), Some("custom-model"));
    assert_eq!(selection.provider_key.as_deref(), Some("custom-provider"));
    assert_eq!(selection.route_api_method.as_deref(), Some("custom-route"));
}

#[test]
fn resolve_swarm_spawn_model_openai_api_prefix_pins_api_route_over_coordinator() {
    // `agents.swarm_model = "openai-api:gpt-5.5"` must spawn agents on GPT-5.5
    // via the OpenAI API key route, regardless of the coordinator's model/auth.
    let selection = resolve_swarm_spawn_selection(
        None,
        Some("openai-api:gpt-5.5".to_string()),
        &coordinator_identity(
            Some("claude-opus-4-8"),
            Some("claude-oauth"),
            Some("claude-oauth"),
        ),
    );

    assert_eq!(selection.model.as_deref(), Some("gpt-5.5"));
    assert_eq!(selection.provider_key.as_deref(), Some("openai-api-key"));
    assert_eq!(
        selection.route_api_method.as_deref(),
        Some("openai-api-key")
    );
}

#[test]
fn resolve_swarm_spawn_model_auth_route_prefixes_pin_expected_routes() {
    for (configured, expected_model, expected_key) in [
        ("openai-api:gpt-5.5", "gpt-5.5", "openai-api-key"),
        ("openai-oauth:gpt-5.5", "gpt-5.5", "openai-oauth"),
        (
            "claude-api:claude-opus-4-8",
            "claude-opus-4-8",
            "anthropic-api-key",
        ),
        (
            "claude-oauth:claude-opus-4-8",
            "claude-opus-4-8",
            "claude-oauth",
        ),
    ] {
        let selection = resolve_swarm_spawn_selection(
            None,
            Some(configured.to_string()),
            &coordinator_identity(
                Some("some-other-model"),
                Some("some-key"),
                Some("some-route"),
            ),
        );
        assert_eq!(
            selection.model.as_deref(),
            Some(expected_model),
            "configured {configured:?} model",
        );
        assert_eq!(
            selection.provider_key.as_deref(),
            Some(expected_key),
            "configured {configured:?} provider_key",
        );
        assert_eq!(
            selection.route_api_method.as_deref(),
            Some(expected_key),
            "configured {configured:?} route_api_method",
        );
    }
}

#[test]
fn resolve_swarm_spawn_model_inherit_sentinel_uses_coordinator_model() {
    for sentinel in ["inherit", "INHERIT", "coordinator", " inherit ", ""] {
        let selection = resolve_swarm_spawn_selection(
            None,
            Some(sentinel.to_string()),
            &coordinator_identity(
                Some("nvidia/llama-3.3-nemotron-super-49b-v1"),
                Some("nvidia"),
                Some("openai-compatible:nvidia-nim"),
            ),
        );

        assert_eq!(
            selection.model.as_deref(),
            Some("nvidia/llama-3.3-nemotron-super-49b-v1"),
            "sentinel {sentinel:?} should inherit coordinator model",
        );
        assert_eq!(
            selection.provider_key.as_deref(),
            Some("nvidia"),
            "sentinel {sentinel:?} should inherit coordinator provider key",
        );
        assert_eq!(
            selection.route_api_method.as_deref(),
            Some("openai-compatible:nvidia-nim"),
            "sentinel {sentinel:?} should inherit coordinator auth route",
        );
    }
}

#[test]
fn resolve_swarm_spawn_model_requested_model_overrides_configured_pin() {
    for requested in ["openai-api:gpt-5.5", "  openai-api:gpt-5.5 \t"] {
        let selection = resolve_swarm_spawn_selection(
            Some(requested.to_string()),
            Some("claude-oauth:claude-opus-4-8".to_string()),
            &coordinator_identity(
                Some("claude-fable-5"),
                Some("claude-oauth"),
                Some("claude-oauth"),
            ),
        );

        assert_eq!(selection.model.as_deref(), Some("gpt-5.5"));
        assert_eq!(selection.provider_key.as_deref(), Some("openai-api-key"));
        assert_eq!(
            selection.route_api_method.as_deref(),
            Some("openai-api-key")
        );
    }
}

#[test]
fn resolve_swarm_spawn_model_requested_inherit_overrides_configured_pin() {
    for requested in [
        "inherit",
        "INHERIT",
        "coordinator",
        " COORDINATOR ",
        " inherit ",
    ] {
        let selection = resolve_swarm_spawn_selection(
            Some(requested.to_string()),
            Some("openai-api:gpt-5.5".to_string()),
            &coordinator_identity(
                Some("claude-fable-5"),
                Some("claude-api"),
                Some("claude-api"),
            ),
        );

        assert_eq!(selection.model.as_deref(), Some("claude-fable-5"));
        assert_eq!(selection.provider_key.as_deref(), Some("claude-api"));
        assert_eq!(selection.route_api_method.as_deref(), Some("claude-api"));
    }
}

#[test]
fn resolve_swarm_spawn_model_requested_matching_coordinator_model_keeps_route() {
    let selection = resolve_swarm_spawn_selection(
        Some(" custom-model ".to_string()),
        Some("openai-api:gpt-5.5".to_string()),
        &coordinator_identity(
            Some("custom-model"),
            Some("custom-provider"),
            Some("custom-route"),
        ),
    );

    assert_eq!(selection.model.as_deref(), Some("custom-model"));
    assert_eq!(selection.provider_key.as_deref(), Some("custom-provider"));
    assert_eq!(selection.route_api_method.as_deref(), Some("custom-route"));
}

#[test]
fn resolve_swarm_spawn_model_blank_requested_model_falls_back_to_config() {
    for requested in ["", "   ", "\t\n"] {
        let selection = resolve_swarm_spawn_selection(
            Some(requested.to_string()),
            Some("openai-api:gpt-5.5".to_string()),
            &coordinator_identity(
                Some("claude-fable-5"),
                Some("claude-oauth"),
                Some("claude-oauth"),
            ),
        );

        assert_eq!(selection.model.as_deref(), Some("gpt-5.5"));
        assert_eq!(selection.provider_key.as_deref(), Some("openai-api-key"));
        assert_eq!(
            selection.route_api_method.as_deref(),
            Some("openai-api-key")
        );
    }
}

#[test]
fn resolve_swarm_spawn_model_omitted_request_trims_configured_model() {
    let selection = resolve_swarm_spawn_selection(
        None,
        Some(" \topenai-api:gpt-5.5 \n".to_string()),
        &coordinator_identity(
            Some("claude-fable-5"),
            Some("claude-oauth"),
            Some("claude-oauth"),
        ),
    );

    assert_eq!(selection.model.as_deref(), Some("gpt-5.5"));
    assert_eq!(selection.provider_key.as_deref(), Some("openai-api-key"));
    assert_eq!(
        selection.route_api_method.as_deref(),
        Some("openai-api-key")
    );
}

#[test]
fn resolve_swarm_spawn_model_blank_requested_model_inherits_when_unconfigured() {
    let selection = resolve_swarm_spawn_selection(
        Some(" \t\n".to_string()),
        None,
        &coordinator_identity(
            Some("custom-model"),
            Some("custom-provider"),
            Some("custom-route"),
        ),
    );

    assert_eq!(selection.model.as_deref(), Some("custom-model"));
    assert_eq!(selection.provider_key.as_deref(), Some("custom-provider"));
    assert_eq!(selection.route_api_method.as_deref(), Some("custom-route"));
}

#[test]
fn swarm_spawn_effort_prefers_explicit_then_config_pin_then_inherit() {
    use crate::server::comm_session::resolve_swarm_spawn_effort;

    // Explicit spawn argument wins over the config pin (#1165).
    assert_eq!(
        resolve_swarm_spawn_effort(Some("low"), Some("medium")),
        Some("low".to_string())
    );
    // A missing or blank spawn argument falls back to `agents.swarm_effort`.
    assert_eq!(
        resolve_swarm_spawn_effort(None, Some("medium")),
        Some("medium".to_string())
    );
    assert_eq!(
        resolve_swarm_spawn_effort(Some("  "), Some(" medium ")),
        Some("medium".to_string())
    );
    // With neither, the worker inherits the provider-wide effort.
    assert_eq!(resolve_swarm_spawn_effort(None, None), None);
    assert_eq!(resolve_swarm_spawn_effort(Some(""), Some("")), None);
}

pub(super) fn routing_catalog() -> Vec<crate::provider::ModelRoute> {
    [
        ("gpt-5.5", "OpenAI", "openai-oauth", true),
        ("gpt-5.5", "OpenAI", "openai-api-key", true),
        ("claude-opus-4-8", "Anthropic", "claude-oauth", false),
    ]
    .into_iter()
    .map(
        |(model, provider, api_method, available)| crate::provider::ModelRoute {
            model: model.into(),
            provider: provider.into(),
            api_method: api_method.into(),
            available,
            detail: String::new(),
            cheapness: None,
        },
    )
    .collect()
}

#[test]
fn routing_admission_rejects_disabled_explicit_including_sentinels_and_blanks() {
    for model in ["gpt-5.5", "inherit", "coordinator", " "] {
        let error = crate::server::comm_session::admit_swarm_spawn_selection(
            Some(model.into()),
            None,
            &coordinator_identity(Some("gpt-5.5"), Some("openai-oauth"), Some("openai-oauth")),
            false,
            None,
            &routing_catalog(),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("swarm_model_selection"),
            "{error}"
        );
    }
}

#[test]
fn routing_admission_rejects_ambiguity_unavailable_and_empty_policy() {
    let coordinator =
        coordinator_identity(Some("gpt-5.5"), Some("openai-oauth"), Some("openai-oauth"));
    let error = crate::server::comm_session::admit_swarm_spawn_selection(
        Some("gpt-5.5".into()),
        None,
        &coordinator,
        true,
        None,
        &routing_catalog(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("openai-oauth:gpt-5.5"));
    assert!(error.to_string().contains("openai-api:gpt-5.5"));
    assert!(
        crate::server::comm_session::admit_swarm_spawn_selection(
            Some("claude-opus-4-8".into()),
            None,
            &coordinator,
            true,
            None,
            &routing_catalog()
        )
        .is_err()
    );
    assert!(
        crate::server::comm_session::admit_swarm_spawn_selection(
            None,
            None,
            &coordinator,
            true,
            Some(&[]),
            &routing_catalog()
        )
        .is_err()
    );
    assert!(
        crate::server::comm_session::admit_swarm_spawn_selection(
            Some(" ".into()),
            None,
            &coordinator,
            true,
            None,
            &routing_catalog()
        )
        .is_err()
    );
}

#[test]
fn routing_admission_pins_concrete_route_and_normalizes_allowlist_aliases() {
    let coordinator =
        coordinator_identity(Some("gpt-5.5"), Some("openai-oauth"), Some("openai-oauth"));
    let admitted = crate::server::comm_session::admit_swarm_spawn_selection(
        Some("openai-api:gpt-5.5".into()),
        Some("inherit".into()),
        &coordinator,
        true,
        Some(&["openai:gpt-5.5".into()]),
        &routing_catalog(),
    )
    .unwrap();
    assert_eq!(admitted.provider_key.as_deref(), Some("openai-api-key"));
    let inherited = crate::server::comm_session::admit_swarm_spawn_selection(
        Some("inherit".into()),
        Some("openai-api:gpt-5.5".into()),
        &coordinator,
        true,
        Some(&["gpt-5.5".into()]),
        &routing_catalog(),
    )
    .unwrap();
    assert_eq!(inherited.provider_key.as_deref(), Some("openai-oauth"));
}

#[test]
fn routing_subtree_cannot_broaden_route_restriction() {
    let inherited = vec!["openai-oauth:gpt-5.5".into()];
    assert!(
        crate::server::comm_session::narrow_swarm_allowed_models(
            Some(&inherited),
            Some(&["gpt-5.5".into()]),
            &routing_catalog()
        )
        .is_err()
    );
    assert_eq!(
        crate::server::comm_session::narrow_swarm_allowed_models(
            Some(&inherited),
            Some(&[]),
            &routing_catalog()
        )
        .unwrap(),
        Some(vec![])
    );
}

#[test]
fn routing_trusted_ancestry_rejects_missing_parent_cross_swarm_and_cycle() {
    let (mut child, _rx) = member("child", Some("s"), "coordinator");
    child.report_back_to_session_id = Some("root".into());
    let mut members = HashMap::from([("child".into(), child)]);
    assert!(crate::server::swarm::trusted_swarm_ancestry(&members, "child").is_err());
    let (root, _root_rx) = member("root", Some("other"), "coordinator");
    members.insert("root".into(), root);
    assert!(crate::server::swarm::trusted_swarm_ancestry(&members, "child").is_err());
    members.get_mut("root").unwrap().swarm_id = Some("s".into());
    assert_eq!(
        crate::server::swarm::trusted_swarm_ancestry(&members, "child").unwrap(),
        vec!["child", "root"]
    );
    members.get_mut("root").unwrap().report_back_to_session_id = Some("child".into());
    assert!(crate::server::swarm::trusted_swarm_ancestry(&members, "child").is_err());
}

#[derive(Clone)]
struct RoutingProbe {
    api_key: bool,
    effort: Option<String>,
    fail_switch: bool,
}
#[async_trait]
impl Provider for RoutingProbe {
    async fn complete(
        &self,
        _: &[Message],
        _: &[ToolDefinition],
        _: &str,
        _: Option<&str>,
    ) -> Result<EventStream> {
        anyhow::bail!("no model requests in routing tests")
    }
    fn name(&self) -> &str {
        "openai"
    }
    fn model(&self) -> String {
        "gpt-5.5".into()
    }
    fn fork(&self) -> Arc<dyn Provider> {
        Arc::new(self.clone())
    }
    fn model_routes(&self) -> Vec<crate::provider::ModelRoute> {
        routing_catalog()
    }
    fn active_resolved_credential(&self) -> Option<jcode_provider_core::ResolvedCredential> {
        Some(if self.api_key {
            jcode_provider_core::ResolvedCredential::ApiKey
        } else {
            jcode_provider_core::ResolvedCredential::Oauth
        })
    }
    fn set_model(&self, _: &str) -> Result<()> {
        if self.fail_switch {
            anyhow::bail!("route initialization failed")
        }
        Ok(())
    }
    fn reasoning_effort(&self) -> Option<String> {
        self.effort.clone()
    }
    fn set_reasoning_effort(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[test]
fn routing_initialized_identity_never_substitutes_requested_provider_or_effort() {
    let actual = RoutingProbe {
        api_key: true,
        effort: Some("medium".into()),
        fail_switch: false,
    };
    let mut expected = crate::server::comm_session::execution_for_route(
        &routing_catalog()[0],
        Some("high".into()),
    );
    let observed = crate::server::comm_session::initialized_swarm_execution(&actual).unwrap();
    assert_eq!(observed.provider_key, "openai-api-key");
    assert_eq!(observed.effort.as_deref(), Some("medium"));
    assert!(crate::server::comm_session::verify_swarm_execution(&actual, &expected).is_err());
    assert!(
        crate::server::comm_session::initialize_swarm_provider(&actual, &expected, true).is_err()
    );
    expected = crate::server::comm_session::execution_for_route(
        &routing_catalog()[1],
        Some("high".into()),
    );
    assert!(crate::server::comm_session::verify_swarm_execution(&actual, &expected).is_err());
    assert!(
        crate::server::comm_session::initialize_swarm_provider(&actual, &expected, true).is_err()
    );
    expected.effort = Some("medium".into());
    assert!(crate::server::comm_session::verify_swarm_execution(&actual, &expected).is_ok());
    let failing = RoutingProbe {
        fail_switch: true,
        ..actual
    };
    assert!(
        crate::server::comm_session::initialize_swarm_provider(&failing, &expected, true).is_err()
    );
}

#[test]
fn routing_current_operator_tightening_intersects_durable_ancestry() {
    let (mut root, _rx) = member("root", Some("s"), "coordinator");
    root.allowed_models = Some(vec!["gpt-5.5".into()]);
    let (mut child, _rx2) = member("child", Some("s"), "agent");
    child.report_back_to_session_id = Some("root".into());
    child.allowed_models = Some(vec!["openai-oauth:gpt-5.5".into()]);
    let members = HashMap::from([("root".into(), root), ("child".into(), child)]);
    let allowed = crate::server::comm_session::effective_swarm_allowed_models(
        &members,
        "child",
        Some(&[]),
        None,
        &routing_catalog(),
    )
    .unwrap();
    assert_eq!(allowed, Some(vec![]));
    assert!(
        crate::server::comm_session::effective_swarm_allowed_models(
            &members,
            "child",
            None,
            Some(&["gpt-5.5".into()]),
            &routing_catalog()
        )
        .is_err()
    );
}

#[test]
fn routing_disabled_omission_preserves_legacy_pin_and_inheritance() {
    let identity = coordinator_identity(Some("custom"), Some("custom-provider"), None);
    let inherited = crate::server::comm_session::admit_swarm_spawn_selection(
        None,
        None,
        &identity,
        false,
        None,
        &[],
    )
    .unwrap();
    assert_eq!(inherited.model.as_deref(), Some("custom"));
    let pinned = crate::server::comm_session::admit_swarm_spawn_selection(
        None,
        Some("openai-api:gpt-5.5".into()),
        &identity,
        false,
        None,
        &[],
    )
    .unwrap();
    assert_eq!(pinned.provider_key.as_deref(), Some("openai-api-key"));
}

#[test]
fn routing_policy_projection_never_broadens_a_lossy_catalog_selector() {
    let mut routes = routing_catalog();
    routes.push(crate::provider::ModelRoute {
        model: "gpt-5.5".into(),
        provider: "Jcode".into(),
        api_method: "jcode-subscription".into(),
        available: true,
        detail: String::new(),
        cheapness: None,
    });
    let (mut root, _rx) = member("root", Some("s"), "coordinator");
    root.allowed_models = Some(vec!["openai-oauth:gpt-5.5".into()]);
    let members = HashMap::from([("root".into(), root)]);
    let allowed = crate::server::comm_session::effective_swarm_allowed_models(
        &members, "root", None, None, &routes,
    )
    .unwrap()
    .unwrap();
    assert_eq!(allowed, vec!["openai-oauth:gpt-5.5"]);
    assert!(!crate::server::comm_session::permitted_route(
        &routes[1],
        Some(&allowed)
    ));
    assert!(!crate::server::comm_session::permitted_route(
        &routes[3],
        Some(&allowed)
    ));
}

#[test]
fn routing_spawn_fingerprints_preserve_absent_empty_and_escaped_policy() {
    use crate::server::comm_session::spawn_routing_fingerprints;
    let absent = spawn_routing_fingerprints(&None, &None, &None).unwrap();
    let empty = spawn_routing_fingerprints(&None, &None, &Some(vec![])).unwrap();
    assert_eq!(absent, ("null".into(), "[null,null,null]".into()));
    assert_eq!(empty, ("[]".into(), "[null,null,[]]".into()));
    assert_ne!(absent, empty);
    let model = Some("route:model\"\nλ".to_string());
    let effort = Some("high".to_string());
    let allowed = Some(vec![model.clone().unwrap()]);
    let (policy, request) = spawn_routing_fingerprints(&model, &effort, &allowed).unwrap();
    assert_eq!(
        serde_json::from_str::<Option<Vec<String>>>(&policy).unwrap(),
        allowed
    );
    assert_eq!(
        serde_json::from_str::<(Option<String>, Option<String>, Option<Vec<String>>)>(&request)
            .unwrap(),
        (model, effort, allowed)
    );
}

#[test]
fn routing_optional_identity_is_absent_for_legacy_provider_without_catalog() {
    let provider = MockProvider;
    assert!(crate::server::comm_session::initialized_swarm_execution(&provider).is_err());
    assert!(crate::server::comm_session::observe_optional_swarm_execution(&provider).is_none());
    let target = crate::server::comm_session::execution_for_route(&routing_catalog()[0], None);
    assert!(crate::server::comm_session::verify_swarm_execution(&provider, &target).is_err());
    assert!(
        crate::server::comm_session::initialize_swarm_provider(&provider, &target, true).is_err()
    );
}
