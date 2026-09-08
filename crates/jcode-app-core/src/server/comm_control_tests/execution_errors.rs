// Error-path checks exercise the same handler boundary as debug socket requests.
async fn execution_debug_graph(fx: &GraphFixture, request: serde_json::Value) -> serde_json::Value {
    let session_id = Arc::new(RwLock::new(fx.coord.clone()));
    let shared_context = Arc::new(RwLock::new(HashMap::new()));
    let provider: Arc<dyn Provider> = Arc::new(TestProvider);
    let ctx = crate::server::debug_swarm_write::DebugSwarmWriteContext {
        session_id: &session_id,
        sessions: &fx.sessions,
        provider_template: &provider,
        swarm_members: &fx.swarm_members,
        swarms_by_id: &fx.swarms_by_id,
        shared_context: &shared_context,
        swarm_plans: &fx.swarm_plans,
        swarm_coordinators: &fx.swarm_coordinators,
    };
    let response = crate::server::debug_swarm_write::maybe_handle_swarm_write_command(
        &format!("swarm:graph:{request}"),
        &ctx,
    )
    .await
    .unwrap()
    .expect("graph command handled");
    serde_json::from_str(&response).unwrap()
}

#[tokio::test]
async fn execution_debug_graph_missing_or_blank_node_ids_are_rejected_without_mutation() {
    let (_env, _runtime) = RuntimeEnvGuard::new();
    let mut fx = graph_fixture_named("debug-invalid-node", "coord-invalid", "worker-invalid").await;
    fx.seed("light", vec![node_spec("task", "implement", &[])])
        .await;
    let before = fx.swarm_plans.read().await[&fx.swarm_id].clone();
    for (op, field) in [
        ("expand", "node_id"),
        ("inject", "gate_id"),
        ("complete", "node_id"),
    ] {
        for value in [
            serde_json::Value::Null,
            serde_json::json!(""),
            serde_json::json!("   "),
        ] {
            let mut request = serde_json::json!({"op": op, "swarm_id": fx.swarm_id});
            request[field] = value;
            let response = execution_debug_graph(&fx, request).await;
            assert_eq!(response["ok"], false, "{response}");
            assert!(
                response["error"].as_str().unwrap().contains("required"),
                "{response}"
            );
            let after = fx.swarm_plans.read().await[&fx.swarm_id].clone();
            assert_eq!(
                jcode_plan::bridge::to_task_graph(&after),
                jcode_plan::bridge::to_task_graph(&before)
            );
            assert_eq!(after.version, before.version);
            assert_eq!(after.participants, before.participants);
            assert_eq!(after.node_meta, before.node_meta);
        }
    }
}

#[tokio::test]
async fn execution_debug_graph_unknown_operation_is_rejected_before_node_admission() {
    let (_env, _runtime) = RuntimeEnvGuard::new();
    let fx = graph_fixture_named("debug-invalid-op", "coord-invalid-op", "worker-invalid-op").await;
    // The shared fixture creates an empty plan. This case checks that invalid
    // commands cannot create one, so remove only that isolated fixture plan.
    fx.swarm_plans.write().await.clear();
    let response = execution_debug_graph(
        &fx,
        serde_json::json!({
            "op": "unsupported", "swarm_id": fx.swarm_id,
        }),
    )
    .await;
    assert_eq!(response["ok"], false);
    assert!(
        response["error"]
            .as_str()
            .unwrap()
            .contains("unknown op 'unsupported'"),
        "{response}"
    );
    assert!(fx.swarm_plans.read().await.is_empty());
}

#[tokio::test]
async fn execution_unverifiable_concrete_worker_preserves_observation_error() {
    let (_env, _runtime) = RuntimeEnvGuard::new();
    let provider: Arc<dyn Provider> = Arc::new(TestProvider);
    let agent = Agent::new(provider.clone(), Registry::new(provider.clone()).await);
    let routing = crate::protocol::SwarmRoutingStatus {
        resolved_execution: Some(routing_execution("gpt-6-astra", Some("medium"))),
        ..Default::default()
    };
    let error = super::observe_node_execution(
        "coord",
        "opaque-worker",
        &routing,
        true,
        &agent,
        &HashMap::new(),
        provider.as_ref(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Cannot verify worker 'opaque-worker' initialized execution"),
        "{error:#}"
    );
}
