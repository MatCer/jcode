// Error-path checks exercise the same handler boundary as debug socket requests.
async fn execution_debug_graph(fx: &GraphFixture, request: serde_json::Value) -> serde_json::Value {
    execution_debug_graph_with_provider(fx, request, &(Arc::new(TestProvider) as Arc<dyn Provider>))
        .await
}

async fn execution_debug_graph_with_provider(
    fx: &GraphFixture,
    request: serde_json::Value,
    provider: &Arc<dyn Provider>,
) -> serde_json::Value {
    let session_id = Arc::new(RwLock::new(fx.coord.clone()));
    let shared_context = Arc::new(RwLock::new(HashMap::new()));
    let ctx = crate::server::debug_swarm_write::DebugSwarmWriteContext {
        session_id: &session_id,
        sessions: &fx.sessions,
        provider_template: provider,
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
async fn execution_debug_seed_surplus_node_id_does_not_claim_or_inherit_parent() {
    let env = RoutingConfigGuard::new();
    env.configure(true, Some(&["gpt-6-astra"]));
    for surplus in [None, Some("parent"), Some("unknown-parent")] {
        let (fx, provider) = routing_fixture("debug-seed-parent-scope").await;
        routing_seed(&fx, &provider, vec![routed_spec("parent", "gpt-6-astra")]).await;
        {
            let mut plans = fx.swarm_plans.write().await;
            let meta = plans
                .get_mut(&fx.swarm_id)
                .unwrap()
                .node_meta
                .get_mut("parent")
                .unwrap();
            meta.allowed_models = Some(vec![]);
            meta.resolved_execution = Some(routing_execution("gpt-6-astra", Some("medium")));
        }
        let before = fx.swarm_plans.read().await[&fx.swarm_id].clone();
        let mut request = serde_json::json!({
            "op": "seed", "swarm_id": fx.swarm_id, "mode": "light",
            "nodes": [{"id": "new-root", "content": "independent root task", "model": "gpt-6-astra"}]
        });
        if let Some(surplus) = surplus {
            request["node_id"] = serde_json::json!(surplus);
        }
        let response = execution_debug_graph_with_provider(&fx, request, &provider).await;
        assert_eq!(response["ok"], true, "{surplus:?}: {response}");
        assert_eq!(response["op"], "seed");
        assert_eq!(response["count"], 1);
        let after = fx.swarm_plans.read().await[&fx.swarm_id].clone();
        assert_eq!(after.version, before.version + 1);
        let graph = jcode_plan::bridge::to_task_graph(&after);
        assert_eq!(graph.len(), 2);
        assert_eq!(
            graph.get("parent"),
            jcode_plan::bridge::to_task_graph(&before).get("parent")
        );
        assert_eq!(after.node_meta["parent"], before.node_meta["parent"]);
        let seeded = graph.get("new-root").unwrap();
        assert_eq!(seeded.content, "independent root task");
        assert!(seeded.parent.is_none());
        assert!(seeded.owner.is_none());
        assert!(seeded.resolved_execution.is_none());
        assert_eq!(
            seeded.allowed_models,
            Some(vec!["openai-oauth:gpt-6-astra".into()])
        );
    }
}

#[tokio::test]
async fn execution_debug_seed_surplus_node_id_cannot_bypass_admission_guards() {
    let env = RoutingConfigGuard::new();
    let (fx, provider) = routing_fixture("debug-seed-guards").await;
    routing_seed(&fx, &provider, vec![routed_spec("parent", "gpt-6-astra")]).await;
    let before = fx.swarm_plans.read().await[&fx.swarm_id].clone();
    for (actor, enabled, allowed, expected_error) in [
        ("outsider", true, None, "Actor must be an existing member"),
        (
            fx.worker.as_str(),
            true,
            None,
            "Recursive swarm spawning is disabled",
        ),
        (
            fx.coord.as_str(),
            false,
            None,
            "Explicit swarm model selection is disabled",
        ),
        (fx.coord.as_str(), true, Some(&[][..]), "prohibited"),
    ] {
        env.configure(enabled, allowed);
        for surplus in [None, Some("parent"), Some("unknown-parent")] {
            let mut request = serde_json::json!({
                "op": "seed", "swarm_id": fx.swarm_id, "actor": actor,
                "nodes": [{"id": "blocked", "content": "must not be seeded", "model": "gpt-6-astra"}]
            });
            if let Some(surplus) = surplus {
                request["node_id"] = serde_json::json!(surplus);
            }
            let response = execution_debug_graph_with_provider(&fx, request, &provider).await;
            assert_eq!(response["ok"], false, "{surplus:?}: {response}");
            assert!(
                response["error"].as_str().unwrap().contains(expected_error),
                "{response}"
            );
            let after = fx.swarm_plans.read().await[&fx.swarm_id].clone();
            assert_eq!(after.items, before.items);
            assert_eq!(after.version, before.version);
            assert_eq!(after.node_meta, before.node_meta);
            assert_eq!(after.participants, before.participants);
            assert_eq!(after.task_progress, before.task_progress);
            assert_eq!(after.mode, before.mode);
        }
    }
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
    assert!(
        error.chain().skip(1).any(|cause| cause.to_string()
            == "Initialized provider identity cannot be verified against its catalog."),
        "original provider observation error must survive the context: {error:#}"
    );
}
