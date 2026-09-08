use jcode_plan::{NodeMeta, ResolvedExecution};

fn routing_execution(model: &str, effort: Option<&str>) -> ResolvedExecution {
    ResolvedExecution {
        model: model.into(),
        provider_key: "openai-oauth".into(),
        api_method: "openai-oauth".into(),
        provider_label: "OpenAI".into(),
        effort: effort.map(str::to_string),
    }
}

#[test]
fn routing_candidate_requires_actual_route_and_explicit_effort() {
    let expected = routing_execution("gpt-6-astra", Some("medium"));
    assert!(super::execution_matches_node(
        &expected,
        &expected,
        Some("medium"),
        false
    ));
    let other_model = routing_execution("gpt-5.6-sol", Some("medium"));
    assert!(!super::execution_matches_node(
        &expected,
        &other_model,
        Some("medium"),
        false
    ));
    let other_effort = routing_execution("gpt-6-astra", Some("high"));
    assert!(!super::execution_matches_node(
        &expected,
        &other_effort,
        Some("medium"),
        false
    ));
    assert!(super::execution_matches_node(
        &expected,
        &other_effort,
        None,
        false
    ));
    assert!(!super::execution_matches_node(
        &expected,
        &other_effort,
        None,
        true
    ));
    let mut other_api = expected.clone();
    other_api.api_method = "openai-api-key".into();
    assert!(!super::execution_matches_node(
        &expected, &other_api, None, false
    ));
    let mut other_provider = expected.clone();
    other_provider.provider_key = "compatible:other".into();
    assert!(!super::execution_matches_node(
        &expected,
        &other_provider,
        None,
        false
    ));
}

#[test]
fn routing_node_effort_override_conflict_is_rejected() {
    let meta = NodeMeta {
        effort: Some("medium".into()),
        ..Default::default()
    };
    assert_eq!(
        super::node_requested_effort(&meta, Some(" medium ".into())).unwrap(),
        Some("medium".into())
    );
    assert!(super::node_requested_effort(&meta, Some("high".into())).is_err());
    assert!(super::node_requested_effort(&meta, Some(" ".into())).is_err());
    assert_eq!(
        super::node_requested_effort(&meta, None).unwrap(),
        meta.effort
    );
}

#[tokio::test]
async fn routing_requeue_keeps_concrete_pin_and_bridge_roundtrip() {
    let mut plan = VersionedPlan::new();
    let mut item = plan_item("routed", "failed", "high", &[]);
    item.assigned_to = Some("old".into());
    plan.items.push(item);
    let meta = NodeMeta {
        model: Some("gpt-6-astra".into()),
        effort: Some("medium".into()),
        resolved_execution: Some(routing_execution("gpt-6-astra", Some("medium"))),
        ..Default::default()
    };
    plan.node_meta.insert("routed".into(), meta.clone());
    let plans = Arc::new(RwLock::new(HashMap::from([(
        "routing-requeue".to_string(),
        plan,
    )])));
    super::requeue_assignment_in_plan(
        plans.write().await.get_mut("routing-requeue").unwrap(),
        "coord",
        "replacement",
        "routed",
        "retry",
    )
    .unwrap();
    let mut plans = plans.write().await;
    let plan = plans.get_mut("routing-requeue").unwrap();
    assert_eq!(plan.node_meta["routed"], meta);
    let graph = jcode_plan::bridge::to_task_graph(plan);
    jcode_plan::bridge::apply_task_graph(plan, &graph);
    assert_eq!(
        plan.node_meta["routed"].resolved_execution,
        meta.resolved_execution
    );
    assert_eq!(plan.items[0].assigned_to.as_deref(), Some("replacement"));
}

struct RoutingConfigGuard {
    _runtime: RuntimeEnvGuard,
    _dir: tempfile::TempDir,
    home: Option<std::ffi::OsString>,
}
impl RoutingConfigGuard {
    fn new() -> Self {
        let (runtime, dir) = RuntimeEnvGuard::new();
        let home = std::env::var_os("JCODE_HOME");
        crate::env::set_var("JCODE_HOME", dir.path());
        let guard = Self {
            _runtime: runtime,
            _dir: dir,
            home,
        };
        guard.configure(true, None);
        guard
    }
    fn configure(&self, enabled: bool, allowed: Option<&[&str]>) {
        let mut text =
            format!("[agents]\nswarm_model_selection = {enabled}\nswarm_model = \"gpt-6-astra\"\n");
        if let Some(allowed) = allowed {
            text.push_str(&format!(
                "swarm_allowed_models = {}\n",
                serde_json::to_string(allowed).unwrap()
            ));
        }
        std::fs::write(self._dir.path().join("config.toml"), text).unwrap();
        crate::config::invalidate_config_cache();
        assert_eq!(
            crate::config::config().agents.swarm_model_selection,
            enabled
        );
    }
}
impl Drop for RoutingConfigGuard {
    fn drop(&mut self) {
        if let Some(home) = self.home.take() {
            crate::env::set_var("JCODE_HOME", home);
        } else {
            crate::env::remove_var("JCODE_HOME");
        }
        crate::config::invalidate_config_cache();
    }
}

struct RoutingProvider {
    model: std::sync::Mutex<String>,
    effort: std::sync::Mutex<Option<String>>,
}
impl RoutingProvider {
    fn new(model: &str, effort: &str) -> Self {
        Self {
            model: std::sync::Mutex::new(model.into()),
            effort: std::sync::Mutex::new(Some(effort.into())),
        }
    }
}
#[async_trait]
impl Provider for RoutingProvider {
    async fn complete(
        &self,
        _: &[Message],
        _: &[ToolDefinition],
        _: &str,
        _: Option<&str>,
    ) -> Result<EventStream> {
        anyhow::bail!("routing fixtures must stay client-attached; no model execution")
    }
    fn name(&self) -> &str {
        "openai"
    }
    fn model(&self) -> String {
        self.model.lock().unwrap().clone()
    }
    fn fork(&self) -> Arc<dyn Provider> {
        Arc::new(Self {
            model: std::sync::Mutex::new(self.model()),
            effort: std::sync::Mutex::new(self.reasoning_effort()),
        })
    }
    fn model_routes(&self) -> Vec<crate::provider::ModelRoute> {
        ["gpt-6-astra", "gpt-5.6-sol", "unavailable"]
            .into_iter()
            .map(|model| crate::provider::ModelRoute {
                model: model.into(),
                provider: "OpenAI".into(),
                api_method: "openai-oauth".into(),
                available: model != "unavailable",
                detail: String::new(),
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
    fn reasoning_effort(&self) -> Option<String> {
        self.effort.lock().unwrap().clone()
    }
    fn set_reasoning_effort(&self, effort: &str) -> Result<()> {
        if !matches!(effort, "low" | "medium" | "high") {
            anyhow::bail!("unsupported effort");
        }
        *self.effort.lock().unwrap() = Some(effort.into());
        Ok(())
    }
}

async fn routing_fixture(name: &str) -> (GraphFixture, Arc<dyn Provider>) {
    let fx = graph_fixture_named(name, &format!("{name}-coord"), &format!("{name}-astra")).await;
    let provider: Arc<dyn Provider> = Arc::new(RoutingProvider::new("gpt-6-astra", "medium"));
    let agent = Agent::new(provider.fork(), Registry::new(provider.fork()).await);
    fx.sessions
        .write()
        .await
        .insert(fx.coord.clone(), Arc::new(Mutex::new(agent)));
    routing_add_worker(&fx, &fx.worker, "gpt-6-astra", "medium").await;
    (fx, provider)
}
async fn routing_add_worker(fx: &GraphFixture, id: &str, model: &str, effort: &str) {
    let provider: Arc<dyn Provider> = Arc::new(RoutingProvider::new(model, effort));
    let agent = Agent::new(provider.clone(), Registry::new(provider.clone()).await);
    fx.sessions
        .write()
        .await
        .insert(id.into(), Arc::new(Mutex::new(agent)));
    fx.swarm_members.write().await.insert(
        id.into(),
        owned_member(id, &fx.swarm_id, "ready", &fx.coord),
    );
    fx.swarms_by_id
        .write()
        .await
        .get_mut(&fx.swarm_id)
        .unwrap()
        .insert(id.into());
    fx.client_connections.write().await.insert(
        id.into(),
        crate::server::ClientConnectionInfo {
            client_id: id.into(),
            session_id: id.into(),
            client_instance_id: None,
            debug_client_id: None,
            connected_at: Instant::now(),
            last_seen: Instant::now(),
            is_processing: false,
            current_tool_name: None,
            terminal_env: Vec::new(),
            disconnect_tx: mpsc::unbounded_channel().0,
        },
    );
}
async fn routing_seed(
    fx: &GraphFixture,
    provider: &Arc<dyn Provider>,
    nodes: Vec<TaskGraphNodeSpec>,
) {
    handle_comm_seed_graph(
        101,
        fx.coord.clone(),
        Some("light".into()),
        nodes,
        &fx.client_tx,
        &fx.swarm_members,
        &fx.swarms_by_id,
        &fx.swarm_plans,
        &fx.swarm_coordinators,
        &fx.event_history,
        &fx.event_counter,
        &fx.swarm_event_tx,
        &fx.sessions,
        provider,
    )
    .await;
}
async fn routing_assign(
    fx: &GraphFixture,
    provider: &Arc<dyn Provider>,
    task: &str,
    target: Option<String>,
    model: Option<&str>,
    effort: Option<&str>,
) {
    handle_comm_assign_task(
        102,
        fx.coord.clone(),
        target,
        Some(task.into()),
        model.map(str::to_string),
        effort.map(str::to_string),
        None,
        &fx.client_tx,
        &fx.sessions,
        provider,
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
}
fn routed_spec(id: &str, model: &str) -> TaskGraphNodeSpec {
    let mut spec = node_spec(id, "implement", &[]);
    spec.model = Some(model.into());
    spec.effort = Some("medium".into());
    spec
}
fn routing_drain_error(fx: &mut GraphFixture) -> String {
    let mut error = None;
    while let Ok(event) = fx.client_rx.try_recv() {
        if let ServerEvent::Error { message, .. } = event {
            error = Some(message);
        }
    }
    error.expect("expected actionable routing error")
}

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
async fn routing_heterogeneous_nodes_reuse_only_compatible_workers_before_affinity() {
    let _env = RoutingConfigGuard::new();
    let (mut fx, provider) = routing_fixture("routing-heterogeneous").await;
    let sol = "routing-sol";
    routing_add_worker(&fx, sol, "gpt-5.6-sol", "medium").await;
    routing_seed(
        &fx,
        &provider,
        vec![
            routed_spec("a", "gpt-6-astra"),
            routed_spec("b", "gpt-5.6-sol"),
        ],
    )
    .await;
    routing_assign(&fx, &provider, "b", None, None, None).await;
    routing_assign(&fx, &provider, "a", None, None, None).await;
    let plans = fx.swarm_plans.read().await;
    let plan = &plans[&fx.swarm_id];
    assert_eq!(
        plan.items
            .iter()
            .find(|i| i.id == "a")
            .unwrap()
            .assigned_to
            .as_deref(),
        Some(fx.worker.as_str())
    );
    assert_eq!(
        plan.items
            .iter()
            .find(|i| i.id == "b")
            .unwrap()
            .assigned_to
            .as_deref(),
        Some(sol)
    );
    assert_eq!(
        plan.node_meta["a"]
            .resolved_execution
            .as_ref()
            .unwrap()
            .model,
        "gpt-6-astra"
    );
    assert_eq!(
        plan.node_meta["b"]
            .resolved_execution
            .as_ref()
            .unwrap()
            .model,
        "gpt-5.6-sol"
    );
    assert_eq!(
        plan.node_meta["b"]
            .resolved_execution
            .as_ref()
            .unwrap()
            .effort
            .as_deref(),
        Some("medium")
    );
    drop(plans);
    let mut actual_reports = 0;
    while let Ok(event) = fx.client_rx.try_recv() {
        if let ServerEvent::CommAssignTaskResponse {
            routing: Some(routing),
            ..
        } = event
        {
            assert_eq!(routing.actual_execution, routing.resolved_execution);
            actual_reports += 1;
        }
    }
    assert_eq!(actual_reports, 2);
}

#[tokio::test]
async fn routing_conflicts_and_incompatible_explicit_reuse_leave_plan_unclaimed() {
    let _env = RoutingConfigGuard::new();
    let (mut fx, provider) = routing_fixture("routing-conflict").await;
    routing_seed(&fx, &provider, vec![routed_spec("sol", "gpt-5.6-sol")]).await;
    for (target, model, effort) in [
        (Some(fx.worker.clone()), None, None),
        (None, Some("gpt-6-astra"), None),
        (None, None, Some("high")),
    ] {
        routing_assign(&fx, &provider, "sol", target, model, effort).await;
        assert!(!routing_drain_error(&mut fx).is_empty());
        let plans = fx.swarm_plans.read().await;
        let plan = &plans[&fx.swarm_id];
        assert!(plan.items[0].assigned_to.is_none());
        assert!(plan.task_progress.is_empty());
        assert!(plan.node_meta["sol"].resolved_execution.is_none());
    }
    assert!(super::try_claim_auto_assign_target(
        &fx.swarm_id,
        &fx.worker
    ));
    super::release_auto_assign_claim(&fx.swarm_id, &fx.worker);
}

#[tokio::test]
async fn routing_invalid_forbidden_unavailable_and_disabled_seed_are_atomic() {
    let env = RoutingConfigGuard::new();
    let (mut fx, provider) = routing_fixture("routing-invalid").await;
    for model in ["missing", "unavailable", " "] {
        routing_seed(&fx, &provider, vec![routed_spec("bad", model)]).await;
        assert!(!routing_drain_error(&mut fx).is_empty());
        assert!(fx.swarm_plans.read().await[&fx.swarm_id].items.is_empty());
    }
    env.configure(true, Some(&["gpt-6-astra"]));
    routing_seed(&fx, &provider, vec![routed_spec("bad", "gpt-5.6-sol")]).await;
    assert!(!routing_drain_error(&mut fx).is_empty());
    env.configure(false, None);
    routing_seed(&fx, &provider, vec![routed_spec("bad", "gpt-6-astra")]).await;
    assert!(routing_drain_error(&mut fx).contains("disabled"));
    assert!(fx.swarm_plans.read().await[&fx.swarm_id].items.is_empty());
}

#[tokio::test]
async fn routing_tightening_preserves_surviving_pin_and_blocks_removed_route() {
    let env = RoutingConfigGuard::new();
    let (fx, provider) = routing_fixture("routing-tightening").await;
    let meta = NodeMeta {
        resolved_execution: Some(routing_execution("gpt-6-astra", Some("medium"))),
        allowed_models: Some(vec!["gpt-6-astra".into(), "gpt-5.6-sol".into()]),
        ..Default::default()
    };
    env.configure(true, Some(&["gpt-6-astra"]));
    let routing = super::admit_node_execution(
        &fx.coord,
        &meta,
        None,
        None,
        None,
        &fx.sessions,
        &fx.swarm_members,
        &provider,
    )
    .await
    .unwrap();
    assert_eq!(routing.resolved_execution, meta.resolved_execution);
    env.configure(true, Some(&["gpt-5.6-sol"]));
    assert!(
        super::admit_node_execution(
            &fx.coord,
            &meta,
            None,
            None,
            None,
            &fx.sessions,
            &fx.swarm_members,
            &provider
        )
        .await
        .is_err()
    );
    env.configure(true, None);
    let denied = NodeMeta {
        allowed_models: Some(vec![]),
        ..meta
    };
    assert!(
        super::admit_node_execution(
            &fx.coord,
            &denied,
            None,
            None,
            None,
            &fx.sessions,
            &fx.swarm_members,
            &provider
        )
        .await
        .is_err()
    );
}

async fn routing_next(
    fx: &GraphFixture,
    provider: &Arc<dyn Provider>,
    spawn: bool,
    model: Option<&str>,
) {
    routing_next_exact(fx, provider, spawn, model, None).await;
}
async fn routing_next_exact(
    fx: &GraphFixture,
    provider: &Arc<dyn Provider>,
    spawn: bool,
    model: Option<&str>,
    task_id: Option<&str>,
) {
    handle_comm_assign_next(
        103,
        fx.coord.clone(),
        None,
        task_id.map(str::to_string),
        None,
        Some(false),
        Some(spawn),
        None,
        model.map(str::to_string),
        None,
        None,
        &fx.client_tx,
        &fx.sessions,
        &Arc::new(RwLock::new(fx.coord.clone())),
        provider,
        &fx.soft_interrupt_queues,
        &fx.client_connections,
        &fx.swarm_members,
        &fx.swarms_by_id,
        &fx.swarm_plans,
        &fx.swarm_coordinators,
        &fx.event_history,
        &fx.event_counter,
        &fx.swarm_event_tx,
        &Arc::new(crate::mcp::SharedMcpPool::from_default_config()),
        &fx.mutation_runtime,
    )
    .await;
}
async fn routing_control(
    fx: &GraphFixture,
    provider: &Arc<dyn Provider>,
    action: &str,
    target: Option<String>,
) {
    handle_comm_task_control(
        104,
        fx.coord.clone(),
        action.into(),
        "task".into(),
        target,
        None,
        &fx.client_tx,
        &fx.sessions,
        provider,
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

#[tokio::test]
async fn routing_assign_next_spawn_disabled_blocks_then_reuses_matching_worker() {
    let _env = RoutingConfigGuard::new();
    let (mut fx, provider) = routing_fixture("routing-next").await;
    routing_seed(&fx, &provider, vec![routed_spec("task", "gpt-5.6-sol")]).await;
    let members_before = fx.swarm_members.read().await.len();
    routing_next(&fx, &provider, false, None).await;
    assert!(routing_drain_error(&mut fx).contains("matching"));
    assert_eq!(fx.swarm_members.read().await.len(), members_before);
    assert!(
        fx.swarm_plans.read().await[&fx.swarm_id].items[0]
            .assigned_to
            .is_none()
    );
    routing_next(&fx, &provider, true, Some("missing-route")).await;
    assert!(!routing_drain_error(&mut fx).is_empty());
    assert_eq!(fx.swarm_members.read().await.len(), members_before);
    routing_add_worker(&fx, "matching-sol", "gpt-5.6-sol", "medium").await;
    routing_next(&fx, &provider, false, None).await;
    assert_eq!(
        fx.swarm_plans.read().await[&fx.swarm_id].items[0]
            .assigned_to
            .as_deref(),
        Some("matching-sol")
    );
}

#[tokio::test]
async fn routing_retry_replace_and_resume_keep_pin_and_revalidate_policy() {
    let env = RoutingConfigGuard::new();
    let (mut fx, provider) = routing_fixture("routing-lifecycle").await;
    routing_seed(&fx, &provider, vec![routed_spec("task", "gpt-6-astra")]).await;
    routing_assign(&fx, &provider, "task", Some(fx.worker.clone()), None, None).await;
    let pin = fx.swarm_plans.read().await[&fx.swarm_id].node_meta["task"]
        .resolved_execution
        .clone()
        .unwrap();
    fx.swarm_plans
        .write()
        .await
        .get_mut(&fx.swarm_id)
        .unwrap()
        .items[0]
        .status = "failed".into();
    routing_control(&fx, &provider, "retry", None).await;
    assert_eq!(
        fx.swarm_plans.read().await[&fx.swarm_id].node_meta["task"]
            .resolved_execution
            .as_ref(),
        Some(&pin)
    );
    fx.swarm_plans
        .write()
        .await
        .get_mut(&fx.swarm_id)
        .unwrap()
        .items[0]
        .status = "failed".into();
    routing_add_worker(&fx, "wrong-replacement", "gpt-5.6-sol", "medium").await;
    routing_control(&fx, &provider, "replace", Some("wrong-replacement".into())).await;
    assert!(routing_drain_error(&mut fx).contains("compatible"));
    assert_eq!(
        fx.swarm_plans.read().await[&fx.swarm_id].items[0]
            .assigned_to
            .as_deref(),
        Some(fx.worker.as_str())
    );
    routing_add_worker(&fx, "right-replacement", "gpt-6-astra", "medium").await;
    routing_control(&fx, &provider, "replace", Some("right-replacement".into())).await;
    assert_eq!(
        fx.swarm_plans.read().await[&fx.swarm_id].items[0]
            .assigned_to
            .as_deref(),
        Some("right-replacement")
    );
    env.configure(true, Some(&["gpt-5.6-sol"]));
    let before = fx.swarm_plans.read().await[&fx.swarm_id].clone();
    routing_control(&fx, &provider, "resume", None).await;
    assert!(routing_drain_error(&mut fx).contains("routing blocked"));
    let plans = fx.swarm_plans.read().await;
    let after = &plans[&fx.swarm_id];
    assert_eq!(after.version, before.version);
    assert_eq!(after.items, before.items);
    assert_eq!(after.task_progress, before.task_progress);
    assert_eq!(
        after.node_meta["task"].resolved_execution.as_ref(),
        Some(&pin)
    );
}

#[tokio::test]
async fn routing_generated_gate_keeps_actor_snapshot_after_author_cleanup() {
    let env = RoutingConfigGuard::new();
    env.configure(true, Some(&["gpt-6-astra"]));
    let (fx, provider) = routing_fixture("routing-gate").await;
    let specs = vec![jcode_plan::dag::NodeSpec::new(
        "parent",
        "work",
        jcode_plan::dag::NodeKind::Implement,
    )];
    let allowed = crate::server::comm_graph::validate_graph_specs(
        &fx.coord,
        &specs,
        &NodeMeta::default(),
        true,
        &fx.sessions,
        &fx.swarm_members,
        &provider,
    )
    .await
    .unwrap();
    let mut graph = jcode_plan::dag::TaskGraph::new(jcode_plan::dag::Mode::Deep);
    let before = graph.clone();
    jcode_plan::dag::seed(&mut graph, specs).unwrap();
    crate::server::comm_graph::restrict_new_graph_nodes(&mut graph, &before, allowed.as_deref());
    assert!(graph.nodes().iter().any(|node| node.is_gate));
    for node in graph.nodes() {
        assert_eq!(node.allowed_models, allowed);
        assert!(node.resolved_execution.is_none());
    }
    let mut plan = VersionedPlan::new();
    jcode_plan::bridge::apply_task_graph(&mut plan, &graph);
    let meta = plan
        .node_meta
        .values()
        .find(|meta| meta.is_gate)
        .unwrap()
        .clone();
    let serialized = serde_json::to_vec(&meta).unwrap();
    env.configure(true, None);
    fx.swarm_members.write().await.remove(&fx.coord);
    fx.sessions.write().await.remove(&fx.coord);
    let replacement = "routing-gate-new-root";
    let mut root = member(replacement, &fx.swarm_id, "ready");
    root.role = "coordinator".into();
    fx.swarm_members
        .write()
        .await
        .insert(replacement.into(), root);
    let restored: NodeMeta = serde_json::from_slice(&serialized).unwrap();
    assert!(
        super::admit_node_execution(
            replacement,
            &restored,
            Some("gpt-5.6-sol".into()),
            None,
            None,
            &fx.sessions,
            &fx.swarm_members,
            &provider
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn routing_stranded_reclaim_waits_for_admission_and_preserves_pin_and_cap() {
    let _env = RoutingConfigGuard::new();
    let (mut fx, provider) = routing_fixture("routing-stranded").await;
    routing_seed(&fx, &provider, vec![routed_spec("task", "gpt-5.6-sol")]).await;
    let pin = routing_execution("gpt-5.6-sol", Some("medium"));
    {
        let mut plans = fx.swarm_plans.write().await;
        let plan = plans.get_mut(&fx.swarm_id).unwrap();
        plan.items[0].assigned_to = Some("removed-worker".into());
        plan.node_meta.get_mut("task").unwrap().resolved_execution = Some(pin.clone());
        plan.task_progress.insert(
            "task".into(),
            crate::plan::SwarmTaskProgress {
                dead_assignee_reclaims: Some(1),
                assigned_session_id: Some("removed-worker".into()),
                ..Default::default()
            },
        );
    }
    let before = fx.swarm_plans.read().await[&fx.swarm_id].clone();
    routing_next(&fx, &provider, false, None).await;
    assert!(routing_drain_error(&mut fx).contains("routing blocked"));
    assert_eq!(
        fx.swarm_plans.read().await[&fx.swarm_id].version,
        before.version
    );
    assert_eq!(
        fx.swarm_plans.read().await[&fx.swarm_id].task_progress,
        before.task_progress
    );
    routing_add_worker(&fx, "stranded-sol", "gpt-5.6-sol", "medium").await;
    routing_next(&fx, &provider, false, None).await;
    let plans = fx.swarm_plans.read().await;
    let plan = &plans[&fx.swarm_id];
    assert_eq!(plan.items[0].assigned_to.as_deref(), Some("stranded-sol"));
    assert_eq!(plan.task_progress["task"].dead_assignee_reclaims, Some(2));
    assert_eq!(
        plan.node_meta["task"].resolved_execution.as_ref(),
        Some(&pin)
    );
}

#[tokio::test]
async fn routing_exact_next_selects_requested_node_not_earlier_ready_node() {
    let _env = RoutingConfigGuard::new();
    let (fx, provider) = routing_fixture("routing-exact-next").await;
    routing_seed(
        &fx,
        &provider,
        vec![
            routed_spec("a", "gpt-6-astra"),
            routed_spec("b", "gpt-6-astra"),
        ],
    )
    .await;
    routing_next_exact(&fx, &provider, true, None, Some("b")).await;
    let plans = fx.swarm_plans.read().await;
    let plan = &plans[&fx.swarm_id];
    assert!(
        plan.items
            .iter()
            .find(|n| n.id == "a")
            .unwrap()
            .assigned_to
            .is_none()
    );
    assert_eq!(
        plan.items
            .iter()
            .find(|n| n.id == "b")
            .unwrap()
            .assigned_to
            .as_deref(),
        Some(fx.worker.as_str())
    );
    assert_eq!(
        plan.node_meta["b"]
            .resolved_execution
            .as_ref()
            .unwrap()
            .effort
            .as_deref(),
        Some("medium")
    );
}

#[tokio::test]
async fn routing_exact_next_rejects_missing_blank_and_blocked_before_spawn_or_claim() {
    let _env = RoutingConfigGuard::new();
    let (mut fx, provider) = routing_fixture("routing-exact-invalid").await;
    let mut blocked = routed_spec("blocked", "gpt-6-astra");
    blocked.depends_on = vec!["a".into()];
    routing_seed(
        &fx,
        &provider,
        vec![routed_spec("a", "gpt-6-astra"), blocked],
    )
    .await;
    let members = fx.swarm_members.read().await.len();
    for task in ["missing", "  ", "blocked"] {
        routing_next_exact(&fx, &provider, true, None, Some(task)).await;
        let error = routing_drain_error(&mut fx);
        assert!(error.contains("task") || error.contains("Task"), "{error}");
        assert_eq!(fx.swarm_members.read().await.len(), members);
        let plans = fx.swarm_plans.read().await;
        let plan = &plans[&fx.swarm_id];
        assert!(plan.items.iter().all(|n| n.assigned_to.is_none()));
        assert!(
            plan.node_meta
                .values()
                .all(|n| n.resolved_execution.is_none())
        );
    }
    routing_next_exact(&fx, &provider, true, None, Some("a")).await;
    let before = fx.swarm_plans.read().await[&fx.swarm_id].clone();
    routing_next_exact(&fx, &provider, true, None, Some("a")).await;
    assert!(routing_drain_error(&mut fx).contains("must be runnable and unassigned"));
    assert_eq!(fx.swarm_members.read().await.len(), members);
    let after = fx.swarm_plans.read().await[&fx.swarm_id].clone();
    assert_eq!(before.items, after.items);
    assert_eq!(before.node_meta, after.node_meta);
}

#[tokio::test]
async fn routing_plan_status_reports_verified_runtime_not_just_pinned_target() {
    let _env = RoutingConfigGuard::new();
    let (mut fx, provider) = routing_fixture("routing-plan-observed").await;
    routing_seed(&fx, &provider, vec![routed_spec("task", "gpt-6-astra")]).await;
    routing_assign(&fx, &provider, "task", None, None, None).await;
    super::super::comm_sync::handle_comm_plan_status(
        901,
        fx.coord.clone(),
        &fx.swarm_members,
        &fx.swarm_plans,
        &fx.sessions,
        &fx.client_tx,
    )
    .await;
    let mut observed = None;
    while let Ok(event) = fx.client_rx.try_recv() {
        if let ServerEvent::CommPlanStatusResponse { id: 901, summary } = event {
            observed = Some(summary);
        }
    }
    let routing = observed.unwrap().node_routing.remove("task").unwrap();
    assert_eq!(
        routing
            .actual_execution
            .as_ref()
            .map(|route| route.model.as_str()),
        Some("gpt-6-astra")
    );
    assert_eq!(
        routing
            .actual_execution
            .as_ref()
            .and_then(|route| route.effort.as_deref()),
        Some("medium")
    );
}

#[tokio::test]
async fn routing_uninitialized_metadata_candidate_cannot_claim_node() {
    let _env = RoutingConfigGuard::new();
    let (mut fx, provider) = routing_fixture("routing-missing-runtime").await;
    routing_seed(&fx, &provider, vec![routed_spec("task", "gpt-6-astra")]).await;
    fx.sessions.write().await.remove(&fx.worker);
    routing_assign(&fx, &provider, "task", Some(fx.worker.clone()), None, None).await;
    let error = routing_drain_error(&mut fx);
    assert!(error.contains("routing blocked:"), "{error}");
    let plans = fx.swarm_plans.read().await;
    let plan = &plans[&fx.swarm_id];
    assert!(plan.items[0].assigned_to.is_none());
    assert!(plan.node_meta["task"].resolved_execution.is_none());
    assert!(plan.task_progress.is_empty());
    assert!(super::try_claim_auto_assign_target(
        &fx.swarm_id,
        &fx.worker
    ));
    super::release_auto_assign_claim(&fx.swarm_id, &fx.worker);
}
