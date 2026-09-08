use super::*;
use crate::protocol::SwarmRoutingStatus;
use crate::server::comm_session as routing;
use jcode_plan::ResolvedExecution;

#[derive(Clone, Copy)]
enum EffortSetter {
    Apply,
    Noop,
    Unsupported,
}

struct EffortProvider {
    effort: std::sync::Mutex<Option<String>>,
    setter: EffortSetter,
}

impl EffortProvider {
    fn new(effort: Option<&str>, setter: EffortSetter) -> Self {
        Self {
            effort: std::sync::Mutex::new(effort.map(str::to_string)),
            setter,
        }
    }
}

#[async_trait]
impl Provider for EffortProvider {
    async fn complete(
        &self,
        _: &[Message],
        _: &[ToolDefinition],
        _: &str,
        _: Option<&str>,
    ) -> Result<EventStream> {
        anyhow::bail!("effort tests must not execute model requests")
    }
    fn name(&self) -> &str {
        "openai"
    }
    fn model(&self) -> String {
        "gpt-5.5".into()
    }
    fn fork(&self) -> Arc<dyn Provider> {
        Arc::new(Self::new(self.reasoning_effort().as_deref(), self.setter))
    }
    fn model_routes(&self) -> Vec<crate::provider::ModelRoute> {
        routing_catalog()
    }
    fn active_resolved_credential(&self) -> Option<jcode_provider_core::ResolvedCredential> {
        Some(jcode_provider_core::ResolvedCredential::Oauth)
    }
    fn set_model(&self, _: &str) -> Result<()> {
        Ok(())
    }
    fn reasoning_effort(&self) -> Option<String> {
        self.effort.lock().unwrap().clone()
    }
    fn set_reasoning_effort(&self, effort: &str) -> Result<()> {
        match self.setter {
            EffortSetter::Apply => *self.effort.lock().unwrap() = Some(effort.into()),
            EffortSetter::Noop => (),
            EffortSetter::Unsupported => anyhow::bail!("unsupported effort"),
        }
        Ok(())
    }
}

fn pin(effort: Option<&str>) -> ResolvedExecution {
    routing::execution_for_route(&routing_catalog()[0], effort.map(str::to_string))
}

fn root_members() -> Arc<RwLock<HashMap<String, SwarmMember>>> {
    let (root, _rx) = member("root", Some("s"), "coordinator");
    Arc::new(RwLock::new(HashMap::from([("root".into(), root)])))
}

struct EffortEnvironment {
    home: Option<TestHome>,
    dir: tempfile::TempDir,
    previous_runtime: Option<std::ffi::OsString>,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl EffortEnvironment {
    fn new(configured_effort: &str) -> Self {
        let lock = crate::storage::lock_test_env();
        let dir = tempfile::TempDir::new().unwrap();
        let home = TestHome::set(dir.path());
        let previous_runtime = std::env::var_os("JCODE_RUNTIME_DIR");
        crate::env::set_var("JCODE_RUNTIME_DIR", dir.path());
        let env = Self {
            home: Some(home),
            dir,
            previous_runtime,
            _lock: lock,
        };
        env.configure(configured_effort);
        env
    }

    fn configure(&self, effort: &str) {
        std::fs::write(
            self.dir.path().join("config.toml"),
            format!("[features]\nmemory = false\n[agents]\nswarm_model_selection = true\nswarm_model = \"openai-oauth:gpt-5.5\"\nswarm_effort = \"{effort}\"\n"),
        ).unwrap();
        crate::config::invalidate_config_cache();
        assert_eq!(
            crate::config::config().agents.swarm_effort.as_deref(),
            Some(effort)
        );
    }
}

impl Drop for EffortEnvironment {
    fn drop(&mut self) {
        match self.previous_runtime.take() {
            Some(value) => crate::env::set_var("JCODE_RUNTIME_DIR", value),
            None => crate::env::remove_var("JCODE_RUNTIME_DIR"),
        }
        drop(self.home.take());
        crate::config::invalidate_config_cache();
    }
}

#[test]
fn routing_pinned_effort_verifier_rejects_omitted_request_mismatch() {
    let provider = EffortProvider::new(Some("medium"), EffortSetter::Noop);
    let result = routing::verify_swarm_execution(&provider, &pin(Some("high")));
    assert!(
        result.is_err(),
        "durable high pin accepted actual medium without a request"
    );
}

#[test]
fn routing_pinned_effort_none_is_not_an_unconstrained_default() {
    let provider = EffortProvider::new(Some("medium"), EffortSetter::Noop);
    let result = routing::verify_swarm_execution(&provider, &pin(None));
    assert!(
        result.is_err(),
        "durable None pin accepted a new medium default"
    );
}

#[test]
fn routing_pinned_effort_initialization_rejects_noop_setter() {
    let provider = EffortProvider::new(Some("medium"), EffortSetter::Noop);
    assert!(routing::initialize_swarm_provider(&provider, &pin(Some("high")), true).is_err());
}

#[test]
fn routing_pinned_effort_initialization_rejects_unsupported_setter() {
    let provider = EffortProvider::new(Some("medium"), EffortSetter::Unsupported);
    assert!(routing::initialize_swarm_provider(&provider, &pin(Some("high")), true).is_err());
}

#[tokio::test]
async fn routing_pinned_effort_admission_preserves_pin_after_config_change() {
    let env = EffortEnvironment::new("high");
    let provider: Arc<dyn Provider> =
        Arc::new(EffortProvider::new(Some("medium"), EffortSetter::Apply));
    let sessions = Arc::new(RwLock::new(HashMap::new()));
    let members = root_members();
    let admitted = routing::admit_swarm_execution(
        "root", None, None, None, None, &sessions, &members, &provider,
    )
    .await
    .unwrap();
    assert_eq!(
        admitted
            .resolved_execution
            .as_ref()
            .unwrap()
            .effort
            .as_deref(),
        Some("high")
    );
    env.configure("low");
    let readmitted = routing::admit_swarm_execution(
        "root",
        None,
        None,
        None,
        admitted.resolved_execution.as_ref(),
        &sessions,
        &members,
        &provider,
    )
    .await
    .unwrap();
    assert_eq!(readmitted.resolved_execution, admitted.resolved_execution);
    assert_eq!(
        provider.reasoning_effort().as_deref(),
        Some("medium"),
        "admission must only mutate its probe"
    );
    assert!(sessions.read().await.is_empty());
    assert_eq!(members.read().await.len(), 1);
}

#[tokio::test]
async fn routing_pinned_effort_unpinned_default_records_observed_effort() {
    let _env = EffortEnvironment::new("high");
    for setter in [EffortSetter::Noop, EffortSetter::Unsupported] {
        for actual in [Some("medium"), None] {
            let provider: Arc<dyn Provider> = Arc::new(EffortProvider::new(actual, setter));
            let sessions = Arc::new(RwLock::new(HashMap::new()));
            let members = root_members();
            let admitted = routing::admit_swarm_execution(
                "root", None, None, None, None, &sessions, &members, &provider,
            )
            .await
            .unwrap();
            assert_eq!(
                admitted.resolved_execution.unwrap().effort.as_deref(),
                actual,
                "best-effort default must pin observed effort, not unsupported configured effort"
            );
            for (requested, pinned) in
                [(Some("high".into()), None), (None, Some(pin(Some("high"))))]
            {
                assert!(
                    routing::admit_swarm_execution(
                        "root",
                        None,
                        requested,
                        None,
                        pinned.as_ref(),
                        &sessions,
                        &members,
                        &provider,
                    )
                    .await
                    .is_err(),
                    "explicit or durable high effort must not inherit an unsupported default"
                );
                assert!(sessions.read().await.is_empty());
                assert_eq!(members.read().await.len(), 1);
                assert!(members.read().await["root"].runtime.routing.is_none());
                assert_eq!(provider.reasoning_effort().as_deref(), actual);
            }
        }
    }
}

#[tokio::test]
async fn routing_pinned_effort_startup_rejects_persisted_mismatch_without_mutation() {
    let env = EffortEnvironment::new("low");
    for expected in [Some("high"), None] {
        let provider: Arc<dyn Provider> =
            Arc::new(EffortProvider::new(Some("medium"), EffortSetter::Noop));
        let agent = Agent::new(provider.clone(), Registry::new(provider.clone()).await);
        let id = agent.session_id().to_string();
        let sessions = Arc::new(RwLock::new(HashMap::from([(
            id.clone(),
            Arc::new(Mutex::new(agent)),
        )])));
        let stored = SwarmRoutingStatus {
            resolved_execution: Some(pin(expected)),
            ..Default::default()
        };
        let path = env.dir.path().join("persisted-routing.json");
        std::fs::write(&path, serde_json::to_vec(&stored).unwrap()).unwrap();
        let restored = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let (mut worker, _rx) = member(&id, Some("s"), "agent");
        worker.runtime.routing = Some(restored);
        let members = Arc::new(RwLock::new(HashMap::from([(id.clone(), worker)])));
        let result =
            routing::verify_swarm_session_startup(&id, &sessions, &members, &provider).await;
        assert!(
            result.is_err(),
            "restored {expected:?} pin accepted actual medium"
        );
        assert_eq!(members.read().await[&id].runtime.routing, Some(stored));
        assert_eq!(sessions.read().await.len(), 1);
        assert_eq!(provider.reasoning_effort().as_deref(), Some("medium"));
    }
}

#[tokio::test]
async fn routing_pinned_effort_headless_rejection_has_no_orphan() {
    let env = EffortEnvironment::new("low");
    for setter in [EffortSetter::Noop, EffortSetter::Unsupported] {
        let provider: Arc<dyn Provider> = Arc::new(EffortProvider::new(Some("medium"), setter));
        let sessions = Arc::new(RwLock::new(HashMap::new()));
        let global_id = Arc::new(RwLock::new(String::new()));
        let members = Arc::new(RwLock::new(HashMap::new()));
        let swarms = Arc::new(RwLock::new(HashMap::new()));
        let coordinators = Arc::new(RwLock::new(HashMap::new()));
        let plans = Arc::new(RwLock::new(HashMap::new()));
        let queues = Arc::new(RwLock::new(HashMap::new()));
        let result = crate::server::headless::create_routed_headless_session(
            &sessions,
            &global_id,
            &provider,
            "create_session",
            &members,
            &swarms,
            &coordinators,
            &plans,
            &queues,
            false,
            None,
            None,
            None,
            Some("high".into()),
            None,
            None,
            crate::server::headless::HeadlessMemoryScope::IsolatedTest,
            Some(SwarmRoutingStatus {
                resolved_execution: Some(pin(Some("high"))),
                ..Default::default()
            }),
        )
        .await;
        assert!(
            result.is_err(),
            "pinned effort mismatch created a headless session"
        );
        assert!(sessions.read().await.is_empty());
        assert!(members.read().await.is_empty());
        assert!(swarms.read().await.is_empty());
        assert!(coordinators.read().await.is_empty());
        assert!(plans.read().await.is_empty());
        assert!(queues.read().await.is_empty());
        assert!(global_id.read().await.is_empty());
        let session_dir = env.dir.path().join("sessions");
        assert!(!session_dir.exists() || std::fs::read_dir(session_dir).unwrap().next().is_none());
    }
}

#[tokio::test]
async fn routing_pinned_effort_spawn_none_does_not_apply_changed_config_default() {
    let env = EffortEnvironment::new("high");
    for (pinned, actual, setter, configured) in [
        (Some(pin(None)), None, EffortSetter::Apply, "high"),
        (None, Some("medium"), EffortSetter::Noop, "high"),
        (None, Some("medium"), EffortSetter::Unsupported, "high"),
        (None, None, EffortSetter::Unsupported, "high"),
        (
            Some(pin(Some("high"))),
            Some("medium"),
            EffortSetter::Apply,
            "low",
        ),
    ] {
        env.configure(configured);
        let expected = pinned.clone().unwrap_or_else(|| pin(actual));
        let provider: Arc<dyn Provider> = Arc::new(EffortProvider::new(actual, setter));
        let sessions = Arc::new(RwLock::new(HashMap::new()));
        let global_id = Arc::new(RwLock::new(String::new()));
        let members = root_members();
        let swarms = Arc::new(RwLock::new(HashMap::new()));
        let coordinators = Arc::new(RwLock::new(HashMap::new()));
        let plans = Arc::new(RwLock::new(HashMap::new()));
        let queues = Arc::new(RwLock::new(HashMap::new()));
        let clients = Arc::new(RwLock::new(HashMap::new()));
        let history = Arc::new(RwLock::new(VecDeque::new()));
        let counter = Arc::new(AtomicU64::new(0));
        let (events, _rx) = broadcast::channel(16);
        let pool = Arc::new(crate::mcp::SharedMcpPool::from_default_config());
        let result = routing::spawn_swarm_agent(
            "root",
            "s",
            None,
            None,
            Some(crate::config::SwarmSpawnMode::Headless),
            None,
            None,
            None,
            pinned,
            Some("pinned-none".into()),
            &sessions,
            &global_id,
            &provider,
            &members,
            &swarms,
            &coordinators,
            &plans,
            &history,
            &counter,
            &events,
            &pool,
            &queues,
            &clients,
        )
        .await
        .unwrap();
        let status = members.read().await[&result]
            .runtime
            .routing
            .clone()
            .unwrap();
        assert_eq!(status.resolved_execution, Some(expected.clone()));
        assert_eq!(
            status.actual_execution,
            Some(expected.clone()),
            "spawn must preserve durable effort and initial default compatibility"
        );
        assert_eq!(
            sessions.read().await[&result]
                .lock()
                .await
                .provider_fork()
                .reasoning_effort(),
            expected.effort
        );
        assert_eq!(
            crate::session::Session::load(&result)
                .unwrap()
                .reasoning_effort,
            expected.effort
        );
    }
}
