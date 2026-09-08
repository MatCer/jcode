#[test]
fn routing_schema_documents_opt_in_and_node_constraints() {
    let schema = CommunicateTool::new().parameters_schema();
    let props = &schema["properties"];
    assert!(
        props["model"]["description"]
            .as_str()
            .unwrap()
            .contains("swarm_model_selection")
    );
    assert_eq!(props["allowed_models"]["type"], "array");
    assert!(
        props["allowed_models"]["description"]
            .as_str()
            .unwrap()
            .contains("empty")
    );
    let nodes = &props["nodes"]["items"]["properties"];
    assert_eq!(nodes["model"]["type"], "string");
    assert_eq!(nodes["effort"]["type"], "string");
    assert!(
        nodes["model"]["description"]
            .as_str()
            .unwrap()
            .contains("authoritative")
    );
}

#[test]
fn routing_parameter_descriptions_stay_under_token_cap() {
    let schema = CommunicateTool::new().parameters_schema();
    for (path, expected_type) in [
        ("/properties/allowed_models", "array"),
        ("/properties/effort", "string"),
        ("/properties/model", "string"),
        ("/properties/nodes", "array"),
        ("/properties/nodes/items/properties/model", "string"),
        ("/properties/nodes/items/properties/effort", "string"),
        ("/properties/prefer_spawn", "boolean"),
        ("/properties/spawn_if_needed", "boolean"),
        ("/properties/task_id", "string"),
    ] {
        let property = schema
            .pointer(path)
            .expect("routing field must remain public");
        assert_eq!(property["type"], expected_type, "{path}");
        let description = property["description"]
            .as_str()
            .expect("routing field must retain actionable guidance");
        let tokens = crate::util::estimate_tokens(description);
        assert!(tokens <= 25, "{path}: {tokens} tokens: {description}");
    }
}

#[test]
fn routing_blocked_is_fatal_even_when_wrapping_legacy_no_workers_or_no_work() {
    for reason in [
        "No ready or completed swarm agents",
        "No runnable unassigned tasks",
    ] {
        assert_eq!(
            super::classify_assign_error(&format!("Task 'route' routing blocked: {reason}")),
            super::AssignErrorAction::Fail,
        );
    }
}

#[test]
fn routing_model_list_distinguishes_disabled_unknown_absent_empty_and_narrowed() {
    use crate::protocol::SwarmRoutingStatus;
    let mut policy = SwarmRoutingStatus {
        selection_enabled: Some(false),
        ..Default::default()
    };
    let output = format_swarm_model_list(None, None, &[], Some(&policy));
    assert!(output.contains("Explicit model selection: disabled"));
    assert!(output.contains("Effective allowed models: unrestricted"));
    policy.selection_enabled = Some(true);
    policy.allowed_models = Some(vec![]);
    let output = format_swarm_model_list(None, None, &[], Some(&policy));
    assert!(output.contains("Explicit model selection: enabled"));
    assert!(output.contains("[] (new execution denied)"));
    policy.allowed_models = Some(vec!["openai-oauth:gpt-6-astra".into()]);
    assert!(
        format_swarm_model_list(None, None, &[], Some(&policy))
            .contains("Effective allowed models: openai-oauth:gpt-6-astra")
    );
    let output = format_swarm_model_list(None, None, &[], None);
    assert!(output.contains("Explicit model selection: unknown"));
    assert!(output.contains("Effective allowed models: unknown"));
}

#[test]
fn routing_graph_inputs_preserve_node_requests_and_assignment_subtree_presence() {
    for action in ["task_graph", "expand_node", "inject_gap"] {
        let input: CommunicateInput = serde_json::from_value(json!({"action":action,"nodes":[
            {"id":"a","content":"work","model":"openai-oauth:gpt-6-astra","effort":"medium"},
            {"id":"b","content":"review","model":"claude-oauth:claude-opus-5","effort":"high"}
        ]}))
        .unwrap();
        let nodes = input.nodes.unwrap();
        assert_eq!(nodes[0].model.as_deref(), Some("openai-oauth:gpt-6-astra"));
        assert_eq!(nodes[1].effort.as_deref(), Some("high"));
    }
    for action in [
        "spawn",
        "assign_task",
        "assign_next",
        "fill_slots",
        "run_plan",
    ] {
        for allowed in [serde_json::Value::Null, json!([]), json!(["gpt-6-astra"])] {
            let input: CommunicateInput =
                serde_json::from_value(json!({"action":action,"allowed_models":allowed})).unwrap();
            assert_eq!(serde_json::to_value(input.allowed_models).unwrap(), allowed);
        }
    }
}

#[tokio::test]
async fn routing_lifecycle_and_graph_top_level_overrides_fail_instead_of_disappearing() {
    let _env = crate::storage::lock_test_env();
    let dir = tempfile::tempdir().unwrap();
    let _socket = EnvGuard::set("JCODE_SOCKET", dir.path().join("unused.sock"));
    let _runtime = EnvGuard::set("JCODE_RUNTIME_DIR", dir.path());
    for action in [
        "retry",
        "replace",
        "resume",
        "task_graph",
        "expand_node",
        "inject_gap",
    ] {
        let error = CommunicateTool::new()
            .execute(
                json!({"action":action,"task_id":"task","node_id":"task","model":"gpt-6-astra"}),
                test_ctx("coord", dir.path()),
            )
            .await
            .expect_err("unsupported override must fail locally");
        assert!(
            error.to_string().contains("routing fields"),
            "{action}: {error}"
        );
    }
}

/// In-process provider double. No credentials or live network are used. Actual
/// routing is read through the same initialized Provider interface as production.
struct PublicRoutingProvider {
    model: std::sync::Mutex<String>,
    effort: std::sync::Mutex<String>,
}
impl PublicRoutingProvider {
    fn new() -> Self {
        Self {
            model: std::sync::Mutex::new("gpt-6-astra".into()),
            effort: std::sync::Mutex::new("medium".into()),
        }
    }
}
#[async_trait]
impl Provider for PublicRoutingProvider {
    async fn complete(
        &self,
        messages: &[Message],
        tools: &[ToolDefinition],
        system: &str,
        resume: Option<&str>,
    ) -> Result<EventStream> {
        DelayedTestProvider {
            delay: Duration::from_millis(100),
        }
        .complete(messages, tools, system, resume)
        .await
    }
    fn name(&self) -> &str {
        "openai"
    }
    fn model(&self) -> String {
        self.model.lock().unwrap().clone()
    }
    fn reasoning_effort(&self) -> Option<String> {
        Some(self.effort.lock().unwrap().clone())
    }
    fn fork(&self) -> Arc<dyn Provider> {
        Arc::new(Self {
            model: std::sync::Mutex::new(self.model()),
            effort: std::sync::Mutex::new(self.reasoning_effort().unwrap()),
        })
    }
    fn model_routes(&self) -> Vec<crate::provider::ModelRoute> {
        ["gpt-6-astra", "gpt-5.6-sol"]
            .into_iter()
            .map(|model| crate::provider::ModelRoute {
                model: model.into(),
                provider: "OpenAI".into(),
                api_method: "openai-oauth".into(),
                available: true,
                detail: "test double".into(),
                cheapness: None,
            })
            .collect()
    }
    fn active_resolved_credential(&self) -> Option<jcode_provider_core::ResolvedCredential> {
        Some(jcode_provider_core::ResolvedCredential::Oauth)
    }
    fn set_route_selection(&self, route: &jcode_provider_core::RouteSelection) -> Result<()> {
        *self.model.lock().unwrap() = route.model.clone();
        Ok(())
    }
    fn set_reasoning_effort(&self, effort: &str) -> Result<()> {
        anyhow::ensure!(
            matches!(effort, "low" | "medium" | "high"),
            "unsupported test effort"
        );
        *self.effort.lock().unwrap() = effort.into();
        Ok(())
    }
}

#[tokio::test]
async fn routing_public_tool_exact_node_spawn_status_policy_and_driver_failures() {
    let _env = crate::storage::lock_test_env();
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let socket = home.path().join("routing.sock");
    let _home = EnvGuard::set("JCODE_HOME", home.path());
    let _runtime = EnvGuard::set("JCODE_RUNTIME_DIR", home.path());
    let _socket = EnvGuard::set("JCODE_SOCKET", &socket);
    let _debug = EnvGuard::set("JCODE_DEBUG_CONTROL", "1");
    // Keep this provider-double fixture offline. Cold memory retrieval otherwise
    // downloads embeddings while the server sampler waits on its cache mutex,
    // blocking this current-thread runtime before the provider stream can finish.
    std::fs::write(home.path().join("config.toml"), "[features]\nmemory = false\n[agents]\nswarm_model_selection = true\nswarm_model = \"gpt-6-astra\"\nswarm_allowed_models = [\"gpt-6-astra\", \"gpt-5.6-sol\"]\n").unwrap();
    crate::config::invalidate_config_cache();
    assert!(!crate::config::config().features.memory);
    let server = Arc::new(Server::new(Arc::new(PublicRoutingProvider::new())));
    let mut server_task = tokio::spawn(async move { server.run().await });
    wait_for_server_socket(&socket, &mut server_task)
        .await
        .unwrap();
    let mut client = RawClient::connect(&socket).await.unwrap();
    client.subscribe(repo.path()).await.unwrap();
    let coord = client.session_id().await.unwrap();
    let ctx = test_ctx(&coord, repo.path());
    let tool = CommunicateTool::new_for_working_dir(Some(repo.path()));
    tool.execute(
        json!({"action":"assign_role","target_session":coord,"role":"coordinator"}),
        ctx.clone(),
    )
    .await
    .unwrap();
    let catalog = tool
        .execute(json!({"action":"list_models"}), ctx.clone())
        .await
        .unwrap();
    assert!(
        catalog.output.contains("Explicit model selection: enabled"),
        "{}",
        catalog.output
    );
    assert!(catalog.output.contains("selector=openai-oauth:gpt-5.6-sol"));
    tool.execute(
        json!({"action":"task_graph","mode":"light","nodes":[
            {"id":"a","content":"Earlier ready task", "model":"gpt-6-astra","effort":"medium"},
            {"id":"b","content":"Exact constrained task", "model":"gpt-5.6-sol","effort":"high"}
        ]}),
        ctx.clone(),
    )
    .await
    .unwrap();
    let assigned = tool.execute(json!({"action":"assign_task","task_id":"b","spawn_if_needed":true,"allowed_models":["gpt-5.6-sol"]}), ctx.clone()).await.unwrap();
    assert!(
        assigned.output.contains(
            "Actual execution: gpt-5.6-sol provider=openai-oauth api=openai-oauth effort=high"
        ),
        "{}",
        assigned.output
    );
    let worker = assigned
        .output
        .lines()
        .next()
        .unwrap()
        .strip_prefix("Task 'b' assigned to ")
        .unwrap();
    let status = tool
        .execute(
            json!({"action":"status","target_session":worker}),
            ctx.clone(),
        )
        .await
        .unwrap();
    assert!(
        status.output.contains(
            "Actual execution: gpt-5.6-sol provider=openai-oauth api=openai-oauth effort=high"
        ) || status
            .output
            .contains("Actual execution: unknown (not verified)"),
        "{}",
        status.output
    );
    assert!(status.output.contains(
        "Pinned execution: gpt-5.6-sol provider=openai-oauth api=openai-oauth effort=high"
    ));
    let plan = tool
        .execute(json!({"action":"plan_status"}), ctx.clone())
        .await
        .unwrap();
    assert!(plan.output.contains("Ready: a"), "{}", plan.output);
    assert!(plan.output.contains("Requested model: gpt-5.6-sol"));
    assert!(
        plan.output.contains("Actual execution: gpt-5.6-sol")
            || plan
                .output
                .contains("Actual execution: unknown (not verified)")
    );
    // The provider double finishes asynchronously. Once its lock is released,
    // status must return a fresh observation, not stay permanently unknown.
    let mut last_status = String::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let idle = tool
                .execute(
                    json!({"action":"status","target_session":worker}),
                    ctx.clone(),
                )
                .await
                .unwrap();
            if idle.output.contains(
                "Actual execution: gpt-5.6-sol provider=openai-oauth api=openai-oauth effort=high",
            ) && idle.output.contains("Activity: idle")
            {
                eprintln!("Verified idle routing observation:\n{}", idle.output);
                break;
            }
            last_status = idle.output;
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!("idle worker must expose fresh actual execution: {error}\n{last_status}")
    });
    let saved = crate::session::Session::load_startup_stub(worker).unwrap();
    assert_eq!(saved.parent_id.as_deref(), Some(coord.as_str()));
    assert_eq!(saved.model.as_deref(), Some("gpt-5.6-sol"));
    assert_eq!(saved.reasoning_effort.as_deref(), Some("high"));
    let session_files = || {
        std::fs::read_dir(home.path().join("sessions"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<std::collections::BTreeSet<_>>()
    };
    let files_before = session_files();
    let rejected_visible = tool.execute(json!({"action":"spawn", "label":"denied visible", "spawn_mode":"visible", "allowed_models":[]}), ctx.clone()).await.unwrap_err();
    assert!(
        rejected_visible.to_string().contains("allowed"),
        "{rejected_visible}"
    );
    assert_eq!(
        session_files(),
        files_before,
        "rejected admission must not persist a prepared worker"
    );
    let members_before = client.comm_list(&coord).await.unwrap().len();
    let denied = tool.execute(json!({"action":"assign_task","task_id":"a","spawn_if_needed":true,"allowed_models":[]}), ctx.clone()).await.unwrap_err();
    assert!(denied.to_string().contains("routing blocked:"), "{denied}");
    assert_eq!(
        client.comm_list(&coord).await.unwrap().len(),
        members_before
    );
    for action in ["fill_slots", "run_plan"] {
        let error = tokio::time::timeout(Duration::from_secs(5), tool.execute(json!({
            "action":action,"concurrency_limit":2,"spawn_if_needed":false,"prefer_spawn":false,"background":false,"retain_agents":true
        }), ctx.clone())).await.expect("routing block must not wait indefinitely").unwrap_err();
        assert!(
            error.to_string().contains("routing blocked:"),
            "{action}: {error}"
        );
    }
    server_task.abort();
    let _ = server_task.await;
    crate::config::invalidate_config_cache();
}
