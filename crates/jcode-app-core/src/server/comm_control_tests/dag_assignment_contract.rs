/// The budget-utilization mechanism: a deep-mode assignment must carry the
/// deep-node execution contract (expand_node for parallel fan-out, or
/// complete_node with a typed artifact) all the way to the worker, and a gate
/// assignment must carry the inject_gap contract. Without this the fan-out
/// budget goes unused because spawned workers never learn the deep workflow.
#[tokio::test]
async fn e2e_deep_assignment_carries_fanout_and_artifact_contract() {
    let (_env, _runtime) = RuntimeEnvGuard::new();
    let mut fx = graph_fixture_named("swarm-deep-directive", "coord-dd", "worker-dd").await;
    fx.seed(
        "deep",
        vec![
            node_spec("explore.a", "explore", &[]),
            node_spec("explore.b", "explore", &[]),
        ],
    )
    .await;

    // Assign a node to the worker via the live path.
    handle_comm_assign_task(
        2,
        fx.coord.clone(),
        Some(fx.worker.clone()),
        Some("explore.a".to_string()),
        None,
        None,
        None,
        &fx.client_tx,
        &fx.sessions,
        &(Arc::new(TestProvider) as Arc<dyn Provider>),
        &fx.soft_interrupt_queues,
        &fx.client_connections,
        &fx.swarm_members,
        &fx.swarms_by_id,
        &fx.swarm_plans,
        &fx.swarm_coordinators,
        &fx.event_history,
        &fx.event_counter,
        &fx.swarm_event_tx,
        &fx.mutation_runtime,
    )
    .await;

    // The worker has no live client, so the assignment ran through
    // spawn_assigned_task_run against the test agent. The durable record of
    // what the worker was told is the assignment summary; assert on the
    // soft-interrupt prompt queued for the worker instead, which carries the
    // full assignment text.
    let queued = {
        let queues = fx.soft_interrupt_queues.read().await;
        queues.get(&fx.worker).and_then(|queue| {
            queue
                .lock()
                .ok()
                .and_then(|pending| pending.first().map(|msg| msg.content.clone()))
        })
    };
    let prompt = queued.expect("deep assignment should queue a task prompt for the worker");
    assert!(
        prompt.contains(jcode_swarm_core::SWARM_DEEP_NODE_MARKER),
        "deep assignment prompt must carry the deep-node contract, got: {prompt}"
    );
    assert!(prompt.contains("action=\"expand_node\", node_id=\"explore.a\""));
    assert!(prompt.contains("action=\"complete_node\", node_id=\"explore.a\""));

    // Light plans must NOT get the directive. Use a fresh fixture: re-seeding
    // the existing non-empty deep plan as light is now rejected (silent rigor
    // downgrade guard), so the light case needs its own swarm.
    let mut lfx = graph_fixture_named("swarm-light-directive", "coord-ld", "worker-ld").await;
    lfx.seed("light", vec![node_spec("light.a", "explore", &[])])
        .await;
    handle_comm_assign_task(
        3,
        lfx.coord.clone(),
        Some(lfx.worker.clone()),
        Some("light.a".to_string()),
        None,
        None,
        None,
        &lfx.client_tx,
        &lfx.sessions,
        &(Arc::new(TestProvider) as Arc<dyn Provider>),
        &lfx.soft_interrupt_queues,
        &lfx.client_connections,
        &lfx.swarm_members,
        &lfx.swarms_by_id,
        &lfx.swarm_plans,
        &lfx.swarm_coordinators,
        &lfx.event_history,
        &lfx.event_counter,
        &lfx.swarm_event_tx,
        &lfx.mutation_runtime,
    )
    .await;
    let light_prompt = {
        let queues = lfx.soft_interrupt_queues.read().await;
        queues.get(&lfx.worker).and_then(|queue| {
            queue
                .lock()
                .ok()
                .and_then(|pending| pending.last().map(|msg| msg.content.clone()))
        })
    }
    .expect("light assignment should also queue a task prompt");
    assert!(
        !light_prompt.contains(jcode_swarm_core::SWARM_DEEP_NODE_MARKER),
        "light assignments must not carry the deep contract"
    );
}

/// Gate dispatch in a deep plan carries the inject_gap contract.
#[tokio::test]
async fn e2e_deep_gate_assignment_carries_inject_gap_contract() {
    let (_env, _runtime) = RuntimeEnvGuard::new();
    let mut fx = graph_fixture_named("swarm-deep-gate", "coord-dg", "worker-dg").await;
    fx.seed("deep", vec![node_spec("root", "explore", &[])])
        .await;

    // Worker owns root (running), expands it so the engine inserts a gate.
    {
        let mut plans = fx.swarm_plans.write().await;
        let plan = plans.get_mut(&fx.swarm_id).unwrap();
        let root = plan.items.iter_mut().find(|i| i.id == "root").unwrap();
        root.status = "running".to_string();
        root.assigned_to = Some(fx.worker.clone());
    }
    handle_comm_expand_node(
        2,
        fx.worker.clone(),
        "root".to_string(),
        vec![node_spec("root.1", "explore", &[])],
        &fx.client_tx,
        &fx.swarm_members,
        &fx.swarms_by_id,
        &fx.swarm_plans,
        &fx.swarm_coordinators,
        &fx.event_history,
        &fx.event_counter,
        &fx.swarm_event_tx,
        &fx.sessions,
        &(Arc::new(TestProvider) as Arc<dyn Provider>),
    )
    .await;

    // Complete the child so the gate becomes ready.
    {
        let mut plans = fx.swarm_plans.write().await;
        let plan = plans.get_mut(&fx.swarm_id).unwrap();
        let child = plan.items.iter_mut().find(|i| i.id == "root.1").unwrap();
        child.status = "running".to_string();
        child.assigned_to = Some(fx.worker.clone());
    }
    handle_comm_complete_node(
        3,
        fx.worker.clone(),
        "root.1".to_string(),
        serde_json::json!({
            "findings": "explored",
            "confidence": "low",
            "what_i_did_not_check": ["error paths"],
        })
        .to_string(),
        &fx.client_tx,
        &fx.swarm_members,
        &fx.swarms_by_id,
        &fx.swarm_plans,
        &fx.swarm_coordinators,
        &fx.event_history,
        &fx.event_counter,
        &fx.swarm_event_tx,
        &fx.sessions,
        &(Arc::new(TestProvider) as Arc<dyn Provider>),
    )
    .await;

    let gate_id = {
        let plans = fx.swarm_plans.read().await;
        let plan = &plans[&fx.swarm_id];
        plan.items
            .iter()
            .find(|i| {
                plan.node_meta
                    .get(&i.id)
                    // The composite's own gate, not the plan-wide root gate.
                    .map(|m| m.is_gate && m.parent.as_deref() == Some("root"))
                    .unwrap_or(false)
            })
            .map(|i| i.id.clone())
            .expect("deep expand should have inserted a gate")
    };

    // Assign the gate to the worker; its prompt must carry the gate contract.
    handle_comm_assign_task(
        4,
        fx.coord.clone(),
        Some(fx.worker.clone()),
        Some(gate_id.clone()),
        None,
        None,
        None,
        &fx.client_tx,
        &fx.sessions,
        &(Arc::new(TestProvider) as Arc<dyn Provider>),
        &fx.soft_interrupt_queues,
        &fx.client_connections,
        &fx.swarm_members,
        &fx.swarms_by_id,
        &fx.swarm_plans,
        &fx.swarm_coordinators,
        &fx.event_history,
        &fx.event_counter,
        &fx.swarm_event_tx,
        &fx.mutation_runtime,
    )
    .await;

    let prompt = {
        let queues = fx.soft_interrupt_queues.read().await;
        queues.get(&fx.worker).and_then(|queue| {
            queue
                .lock()
                .ok()
                .and_then(|pending| pending.last().map(|msg| msg.content.clone()))
        })
    }
    .expect("gate assignment should queue a task prompt for the worker");
    assert!(prompt.contains(jcode_swarm_core::SWARM_DEEP_NODE_MARKER));
    assert!(
        prompt.contains(&format!("action=\"inject_gap\", gate_id=\"{gate_id}\"")),
        "gate prompt must carry the inject_gap contract, got: {prompt}"
    );
    // The gate also sees the child's artifact (forward dataflow) including the
    // unexplored surface it is supposed to mine.
    assert!(prompt.contains("error paths"));
    // The child completed with LOW confidence, so the gate directive must name
    // it as a priority probe target (the engine rejects a pass over it).
    assert!(
        prompt.contains("PRIORITY") && prompt.contains("root.1"),
        "gate prompt must call out the low-confidence sibling, got: {prompt}"
    );
}
