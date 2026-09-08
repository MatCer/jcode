#[tokio::test]
async fn routing_busy_worker_cannot_claim_using_cached_actual() {
    let _env = RoutingConfigGuard::new();
    let (mut fx, provider) = routing_fixture("routing-busy-cache").await;
    routing_seed(&fx, &provider, vec![routed_spec("task", "gpt-6-astra")]).await;
    let before = fx.swarm_plans.read().await[&fx.swarm_id].clone();
    fx.swarm_members
        .write()
        .await
        .get_mut(&fx.worker)
        .unwrap()
        .runtime
        .routing = Some(crate::protocol::SwarmRoutingStatus {
        actual_execution: Some(routing_execution("gpt-6-astra", Some("medium"))),
        ..Default::default()
    });
    let agent = fx.sessions.read().await[&fx.worker].clone();
    let mut locked = agent.lock().await;
    let route = provider
        .model_routes()
        .into_iter()
        .find(|r| r.model == "gpt-5.6-sol")
        .unwrap();
    locked
        .set_route_selection(&jcode_provider_core::RouteSelection::from_model_route(
            &route,
        ))
        .unwrap();
    routing_assign(&fx, &provider, "task", Some(fx.worker.clone()), None, None).await;
    assert!(routing_drain_error(&mut fx).contains("routing blocked"));
    let after = fx.swarm_plans.read().await[&fx.swarm_id].clone();
    assert_eq!(before.items, after.items);
    assert_eq!(before.node_meta, after.node_meta);
    assert_eq!(before.task_progress, after.task_progress);
    assert!(super::try_claim_auto_assign_target(
        &fx.swarm_id,
        &fx.worker
    ));
    super::release_auto_assign_claim(&fx.swarm_id, &fx.worker);
}

#[tokio::test]
async fn routing_busy_status_never_reports_cached_actual() {
    let _env = RoutingConfigGuard::new();
    let (mut fx, provider) = routing_fixture("routing-status-busy-cache").await;
    routing_seed(&fx, &provider, vec![routed_spec("task", "gpt-6-astra")]).await;
    routing_assign(&fx, &provider, "task", Some(fx.worker.clone()), None, None).await;
    let cached = routing_execution("gpt-6-astra", Some("medium"));
    fx.swarm_members
        .write()
        .await
        .get_mut(&fx.worker)
        .unwrap()
        .runtime
        .routing = Some(crate::protocol::SwarmRoutingStatus {
        actual_execution: Some(cached.clone()),
        resolved_execution: Some(cached.clone()),
        ..Default::default()
    });
    let agent = fx.sessions.read().await[&fx.worker].clone();
    let mut locked = agent.lock().await;
    let route = provider
        .model_routes()
        .into_iter()
        .find(|r| r.model == "gpt-5.6-sol")
        .unwrap();
    locked
        .set_route_selection(&jcode_provider_core::RouteSelection::from_model_route(
            &route,
        ))
        .unwrap();
    super::super::comm_sync::handle_comm_status(
        902,
        fx.coord.clone(),
        fx.worker.clone(),
        &fx.sessions,
        &fx.swarm_members,
        &fx.client_connections,
        &crate::server::FileTouchService::new(),
        &fx.client_tx,
    )
    .await;
    let mut observed = None;
    while let Ok(event) = fx.client_rx.try_recv() {
        if let ServerEvent::CommStatusResponse { id: 902, snapshot } = event {
            observed = snapshot.routing;
        }
    }
    let observed = observed.unwrap();
    assert_eq!(observed.resolved_execution, Some(cached));
    assert!(observed.actual_execution.is_none());
    super::super::comm_sync::handle_comm_plan_status(
        903,
        fx.coord.clone(),
        &fx.swarm_members,
        &fx.swarm_plans,
        &fx.sessions,
        &fx.client_tx,
    )
    .await;
    let mut plan_observed = None;
    while let Ok(event) = fx.client_rx.try_recv() {
        if let ServerEvent::CommPlanStatusResponse { id: 903, summary } = event {
            plan_observed = Some(summary);
        }
    }
    assert!(
        plan_observed.unwrap().node_routing["task"]
            .actual_execution
            .is_none()
    );
    drop(locked);
    super::super::comm_sync::handle_comm_status(
        904,
        fx.coord.clone(),
        fx.worker.clone(),
        &fx.sessions,
        &fx.swarm_members,
        &fx.client_connections,
        &crate::server::FileTouchService::new(),
        &fx.client_tx,
    )
    .await;
    let mut idle_observed = None;
    while let Ok(event) = fx.client_rx.try_recv() {
        if let ServerEvent::CommStatusResponse { id: 904, snapshot } = event {
            idle_observed = snapshot.routing;
        }
    }
    let idle = idle_observed.unwrap();
    assert_eq!(idle.resolved_execution.unwrap().model, "gpt-6-astra");
    assert_eq!(idle.actual_execution.unwrap().model, "gpt-5.6-sol");
}

#[tokio::test]
async fn routing_assignment_rechecks_model_effort_and_policy_at_commit() {
    let env = RoutingConfigGuard::new();
    for change in ["model", "effort", "policy"] {
        env.configure(true, None);
        let (mut fx, provider) = routing_fixture(&format!("routing-claim-race-{change}")).await;
        routing_seed(&fx, &provider, vec![routed_spec("task", "gpt-6-astra")]).await;
        let before = fx.swarm_plans.read().await[&fx.swarm_id].clone();
        let plans = fx.swarm_plans.read().await;
        let mut assignment = Box::pin(routing_assign(&fx, &provider, "task", None, None, None));
        // Reads/admission/selection finish synchronously. The held read lock
        // prevents the final plan write, giving a deterministic commit barrier.
        assert!(futures::poll!(assignment.as_mut()).is_pending());
        let agent = fx.sessions.read().await[&fx.worker].clone();
        {
            let mut agent = agent
                .try_lock()
                .expect("selection must not bind stale observation");
            match change {
                "model" => {
                    let route = provider
                        .model_routes()
                        .into_iter()
                        .find(|r| r.model == "gpt-5.6-sol")
                        .unwrap();
                    agent
                        .set_route_selection(
                            &jcode_provider_core::RouteSelection::from_model_route(&route),
                        )
                        .unwrap();
                }
                "effort" => {
                    agent.set_reasoning_effort("high").unwrap();
                }
                "policy" => env.configure(true, Some(&["gpt-5.6-sol"])),
                _ => unreachable!(),
            }
        }
        drop(plans);
        assignment.await;
        assert!(routing_drain_error(&mut fx).contains("routing"), "{change}");
        let after = fx.swarm_plans.read().await[&fx.swarm_id].clone();
        assert_eq!(before.items, after.items, "{change}");
        assert_eq!(before.node_meta, after.node_meta, "{change}");
        assert_eq!(before.task_progress, after.task_progress, "{change}");
        assert_eq!(before.version, after.version, "{change}");
        assert!(
            super::try_claim_auto_assign_target(&fx.swarm_id, &fx.worker),
            "{change}"
        );
        super::release_auto_assign_claim(&fx.swarm_id, &fx.worker);
    }
}

#[tokio::test]
async fn routing_cancelled_assignment_releases_precommit_auto_claim() {
    let _env = RoutingConfigGuard::new();
    let (mut fx, provider) = routing_fixture("routing-cancel-claim").await;
    routing_seed(&fx, &provider, vec![routed_spec("task", "gpt-6-astra")]).await;
    while fx.client_rx.try_recv().is_ok() {}
    let before = fx.swarm_plans.read().await[&fx.swarm_id].clone();
    let plans = fx.swarm_plans.read().await;
    let mut assignment = Box::pin(routing_assign(&fx, &provider, "task", None, None, None));
    assert!(futures::poll!(assignment.as_mut()).is_pending());
    assert!(!super::try_claim_auto_assign_target(
        &fx.swarm_id,
        &fx.worker
    ));
    drop(assignment);
    drop(plans);
    routing_assign(&fx, &provider, "task", None, None, None).await;
    assert!(super::try_claim_auto_assign_target(
        &fx.swarm_id,
        &fx.worker
    ));
    super::release_auto_assign_claim(&fx.swarm_id, &fx.worker);
    let after = fx.swarm_plans.read().await[&fx.swarm_id].clone();
    assert_eq!(before.items, after.items);
    assert_eq!(before.node_meta, after.node_meta);
    assert_eq!(before.task_progress, after.task_progress);
    assert!(fx.sessions.read().await[&fx.worker].try_lock().is_ok());
    for _ in 0..2 {
        let response = tokio::time::timeout(std::time::Duration::from_secs(2), fx.client_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(response, ServerEvent::Error { message, .. } if message.contains("cancelled before dispatch"))
        );
    }
    // Identical retries get an actionable terminal response, not an orphaned
    // in-flight waiter that waits forever.
    routing_assign(&fx, &provider, "task", None, None, None).await;
    assert!(routing_drain_error(&mut fx).contains("cancelled before dispatch"));
}

#[tokio::test]
async fn routing_cancelled_after_commit_preserves_dispatch_and_duplicate_success() {
    let _env = RoutingConfigGuard::new();
    let (mut fx, provider) = routing_fixture("routing-cancel-committed").await;
    routing_seed(&fx, &provider, vec![routed_spec("task", "gpt-6-astra")]).await;
    while fx.client_rx.try_recv().is_ok() {}
    let persistence_lock = crate::server::swarm_operation_lock(&fx.swarm_id);
    let persistence = persistence_lock.lock().await;
    let mut assignment = Box::pin(routing_assign(&fx, &provider, "task", None, None, None));
    assert!(futures::poll!(assignment.as_mut()).is_pending());
    let committed = fx.swarm_plans.read().await[&fx.swarm_id].clone();
    assert_eq!(
        committed.items[0].assigned_to.as_deref(),
        Some(fx.worker.as_str())
    );
    assert!(committed.node_meta["task"].resolved_execution.is_some());
    let agent = fx.sessions.read().await[&fx.worker].clone();
    let queue = agent.lock().await.soft_interrupt_queue();
    assert_eq!(queue.lock().unwrap().len(), 1);
    routing_assign(&fx, &provider, "task", None, None, None).await;
    drop(assignment);
    drop(persistence);
    for _ in 0..2 {
        let response = tokio::time::timeout(std::time::Duration::from_secs(2), fx.client_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(response, ServerEvent::CommAssignTaskResponse { task_id, .. } if task_id == "task")
        );
    }
    routing_assign(&fx, &provider, "task", None, None, None).await;
    assert!(matches!(
        fx.client_rx.try_recv().unwrap(),
        ServerEvent::CommAssignTaskResponse { .. }
    ));
    assert_eq!(
        queue.lock().unwrap().len(),
        1,
        "replay must not dispatch twice"
    );
    let after = fx.swarm_plans.read().await[&fx.swarm_id].clone();
    assert_eq!(committed.items, after.items);
    assert_eq!(committed.node_meta, after.node_meta);
    assert_eq!(committed.version, after.version);
}

#[tokio::test]
async fn routing_graph_actor_rechecks_model_effort_and_policy_at_commit() {
    let env = RoutingConfigGuard::new();
    for change in ["model", "effort", "policy"] {
        env.configure(true, None);
        let (mut fx, provider) = routing_fixture(&format!("routing-graph-race-{change}")).await;
        routing_seed(&fx, &provider, vec![routed_spec("task", "gpt-6-astra")]).await;
        let before = fx.swarm_plans.read().await[&fx.swarm_id].clone();
        let plans = fx.swarm_plans.read().await;
        let mut complete = Box::pin(handle_comm_complete_node(
            105,
            fx.worker.clone(),
            "task".into(),
            "{}".into(),
            &fx.client_tx,
            &fx.swarm_members,
            &fx.swarms_by_id,
            &fx.swarm_plans,
            &fx.swarm_coordinators,
            &fx.event_history,
            &fx.event_counter,
            &fx.swarm_event_tx,
            &fx.sessions,
            &provider,
        ));
        assert!(futures::poll!(complete.as_mut()).is_pending());
        routing_change_at_barrier(&fx, &provider, &env, change).await;
        drop(plans);
        complete.await;
        assert!(!routing_drain_error(&mut fx).is_empty(), "{change}");
        let after = fx.swarm_plans.read().await[&fx.swarm_id].clone();
        assert_eq!(before.items, after.items, "{change}");
        assert_eq!(before.node_meta, after.node_meta, "{change}");
        assert_eq!(before.version, after.version, "{change}");
    }
}

async fn routing_change_at_barrier(
    fx: &GraphFixture,
    provider: &Arc<dyn Provider>,
    env: &RoutingConfigGuard,
    change: &str,
) {
    let agent = fx.sessions.read().await[&fx.worker].clone();
    let mut agent = agent.try_lock().unwrap();
    match change {
        "model" => {
            let route = provider
                .model_routes()
                .into_iter()
                .find(|r| r.model == "gpt-5.6-sol")
                .unwrap();
            agent
                .set_route_selection(&jcode_provider_core::RouteSelection::from_model_route(
                    &route,
                ))
                .unwrap();
        }
        "effort" => {
            agent.set_reasoning_effort("high").unwrap();
        }
        "policy" => env.configure(true, Some(&["gpt-5.6-sol"])),
        _ => panic!("unknown fixture change"),
    }
}

#[tokio::test]
async fn routing_task_control_rechecks_model_effort_and_policy_at_commit() {
    let env = RoutingConfigGuard::new();
    for change in ["model", "effort", "policy"] {
        env.configure(true, None);
        let (mut fx, provider) = routing_fixture(&format!("routing-control-race-{change}")).await;
        routing_seed(&fx, &provider, vec![routed_spec("task", "gpt-6-astra")]).await;
        routing_assign(&fx, &provider, "task", Some(fx.worker.clone()), None, None).await;
        let before = fx.swarm_plans.read().await[&fx.swarm_id].clone();
        let plans = fx.swarm_plans.read().await;
        let mut control = Box::pin(routing_control(&fx, &provider, "start", None));
        assert!(futures::poll!(control.as_mut()).is_pending());
        routing_change_at_barrier(&fx, &provider, &env, change).await;
        drop(plans);
        control.await;
        assert!(
            routing_drain_error(&mut fx).contains("routing blocked"),
            "{change}"
        );
        let after = fx.swarm_plans.read().await[&fx.swarm_id].clone();
        assert_eq!(before.items, after.items, "{change}");
        assert_eq!(before.node_meta, after.node_meta, "{change}");
        assert_eq!(before.task_progress, after.task_progress, "{change}");
        assert_eq!(before.version, after.version, "{change}");
    }
}

#[tokio::test]
async fn routing_queued_client_rejects_model_effort_and_policy_changes_without_consuming_work() {
    let env = RoutingConfigGuard::new();
    for change in ["model", "effort", "policy"] {
        env.configure(true, None);
        let (fx, provider) = routing_fixture(&format!("routing-consume-{change}")).await;
        routing_seed(&fx, &provider, vec![routed_spec("task", "gpt-6-astra")]).await;
        routing_assign(&fx, &provider, "task", Some(fx.worker.clone()), None, None).await;
        let before = fx.swarm_plans.read().await[&fx.swarm_id].clone();
        routing_change_at_barrier(&fx, &provider, &env, change).await;
        let agent = fx.sessions.read().await[&fx.worker].clone();
        let queue = agent.lock().await.soft_interrupt_queue();
        let queued = queue.lock().unwrap().len();
        assert!(queued > 0);
        let message_count = agent.lock().await.message_count();
        let error = super::super::client_lifecycle::process_routed_message_streaming_mpsc(
            agent.clone(),
            "continue queued assignment",
            Vec::new(),
            None,
            fx.client_tx.clone(),
            &fx.worker,
            &fx.swarm_members,
            &fx.swarm_plans,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("routing blocked"),
            "{change}: {error}"
        );
        assert_eq!(queue.lock().unwrap().len(), queued, "{change}");
        assert_eq!(
            agent.lock().await.message_count(),
            message_count,
            "{change}"
        );
        let after = fx.swarm_plans.read().await[&fx.swarm_id].clone();
        assert_eq!(before.items, after.items, "{change}");
        assert_eq!(before.node_meta, after.node_meta, "{change}");
        assert_eq!(before.task_progress, after.task_progress, "{change}");
        env.configure(true, None);
        {
            let mut restored = agent.lock().await;
            let route = provider
                .model_routes()
                .into_iter()
                .find(|r| r.model == "gpt-6-astra")
                .unwrap();
            restored
                .set_route_selection(&jcode_provider_core::RouteSelection::from_model_route(
                    &route,
                ))
                .unwrap();
            restored.set_reasoning_effort("medium").unwrap();
        }
        let error = super::super::client_lifecycle::process_routed_message_streaming_mpsc(
            agent.clone(),
            "retry retained assignment",
            Vec::new(),
            None,
            fx.client_tx.clone(),
            &fx.worker,
            &fx.swarm_members,
            &fx.swarm_plans,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("routing fixtures"),
            "recovered route must reach fixture provider: {error}"
        );
        assert!(agent.lock().await.message_count() > message_count);
    }
}

#[tokio::test]
async fn routing_ordinary_client_message_ignores_historical_member_pin() {
    let env = RoutingConfigGuard::new();
    let (fx, _provider) = routing_fixture("routing-ordinary-client").await;
    fx.swarm_members
        .write()
        .await
        .get_mut(&fx.worker)
        .unwrap()
        .runtime
        .routing = Some(crate::protocol::SwarmRoutingStatus {
        resolved_execution: Some(routing_execution("gpt-5.6-sol", Some("high"))),
        ..Default::default()
    });
    env.configure(true, Some(&[]));
    let agent = fx.sessions.read().await[&fx.worker].clone();
    let error = super::super::client_lifecycle::process_routed_message_streaming_mpsc(
        agent,
        "ordinary user message",
        Vec::new(),
        None,
        fx.client_tx.clone(),
        &fx.worker,
        &fx.swarm_members,
        &fx.swarm_plans,
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("routing fixtures"),
        "ordinary message must reach fixture provider: {error}"
    );
}
