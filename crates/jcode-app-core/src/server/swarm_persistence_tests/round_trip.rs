//! Durable member/plan round trips and routing hydration compatibility.

use super::*;

#[test]
fn persisted_swarm_state_round_trips_and_marks_running_stale() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let _env = test_env(&dir);

    let mut plans = HashMap::new();
    plans.insert(
        "swarm-alpha".to_string(),
        VersionedPlan {
            items: vec![crate::plan::PlanItem {
                content: "do thing".to_string(),
                status: "running".to_string(),
                priority: "high".to_string(),
                id: "task-1".to_string(),
                subsystem: None,
                file_scope: Vec::new(),
                blocked_by: Vec::new(),
                assigned_to: Some("session-1".to_string()),
            }],
            version: 3,
            participants: ["session-1".to_string(), "session-2".to_string()]
                .into_iter()
                .collect(),
            task_progress: HashMap::from([(
                "task-1".to_string(),
                SwarmTaskProgress {
                    assigned_session_id: Some("session-1".to_string()),
                    assignment_summary: Some("do thing".to_string()),
                    assigned_at_unix_ms: Some(10),
                    started_at_unix_ms: Some(20),
                    last_heartbeat_unix_ms: Some(30),
                    last_detail: Some("tool start: read".to_string()),
                    last_checkpoint_unix_ms: Some(40),
                    checkpoint_summary: Some("tool done: read".to_string()),
                    completed_at_unix_ms: None,
                    stale_since_unix_ms: None,
                    heartbeat_count: Some(2),
                    checkpoint_count: Some(1),
                    no_artifact_requeues: None,
                    dead_assignee_reclaims: None,
                },
            )]),
            mode: "light".to_string(),
            node_meta: HashMap::new(),
        },
    );
    let coordinators = HashMap::from([("swarm-alpha".to_string(), "session-2".to_string())]);
    let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();
    let members = vec![SwarmMember {
        session_id: "session-1".to_string(),
        event_tx,
        event_txs: HashMap::new(),
        working_dir: Some(PathBuf::from("/tmp/swarm-alpha")),
        swarm_id: Some("swarm-alpha".to_string()),
        swarm_enabled: true,
        status: "running".to_string(),
        detail: Some("writing tests".to_string()),
        friendly_name: Some("fox".to_string()),
        report_back_to_session_id: Some("session-2".to_string()),
        allowed_models: None,
        latest_completion_report: None,
        role: "agent".to_string(),
        joined_at: Instant::now(),
        last_status_change: Instant::now(),
        is_headless: true,
        output_tail: None,
        todo_progress: None,
        todo_items: Vec::new(),
        runtime: crate::protocol::SwarmMemberRuntime::default(),
        task_label: None,
    }];

    persist_swarm_state(
        "swarm-alpha",
        plans.get("swarm-alpha"),
        coordinators.get("swarm-alpha").map(String::as_str),
        &members,
    );
    let loaded = load_runtime_state();

    let loaded_plan = loaded.plans.get("swarm-alpha").expect("loaded plan");
    assert_eq!(loaded_plan.version, 3);
    assert_eq!(loaded_plan.items.len(), 1);
    assert_eq!(loaded_plan.items[0].status, "running_stale");
    let progress = loaded_plan
        .task_progress
        .get("task-1")
        .expect("task progress");
    assert_eq!(progress.assigned_session_id.as_deref(), Some("session-1"));
    assert_eq!(
        progress.checkpoint_summary.as_deref(),
        Some("tool done: read")
    );
    assert!(progress.stale_since_unix_ms.is_some());
    assert_eq!(
        loaded.coordinators.get("swarm-alpha"),
        Some(&"session-2".to_string())
    );
    let recovered_member = loaded.members.get("session-1").expect("recovered member");
    assert_eq!(recovered_member.role, "agent");
    assert_eq!(
        recovered_member.report_back_to_session_id.as_deref(),
        Some("session-2")
    );
    assert_eq!(recovered_member.status, "crashed");
    assert_eq!(
        recovered_member.detail.as_deref(),
        Some("writing tests (recovered after reload while running)")
    );
    assert_eq!(
        loaded.swarms_by_id.get("swarm-alpha"),
        Some(&HashSet::from(["session-1".to_string()]))
    );
}

#[test]
fn routing_member_policy_json_and_hydration_preserve_absent_empty_and_narrowed() {
    for allowed in [
        None,
        Some(vec![]),
        Some(vec!["openai-oauth:gpt-5.5".to_string()]),
    ] {
        let payload = serde_json::json!({"session_id":"child", "working_dir":null,
            "swarm_id":"routing-test", "swarm_enabled":true, "status":"ready",
            "detail":null,"task_label":null,"friendly_name":null,
            "report_back_to_session_id":"root", "latest_completion_report":null,
            "role":"agent", "is_headless":true});
        let mut record: SwarmMemberRecord = serde_json::from_value(payload).unwrap();
        assert_eq!(record.allowed_models, None);
        record.allowed_models = allowed.clone();
        let record = serde_json::from_str(&serde_json::to_string(&record).unwrap()).unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut member = SwarmMember::from_record(record, tx);
        let target = jcode_plan::ResolvedExecution {
            model: "gpt-5.5".into(),
            provider_key: "openai-oauth".into(),
            api_method: "openai-oauth".into(),
            provider_label: "OpenAI".into(),
            effort: Some("medium".into()),
        };
        member.runtime.routing = Some(crate::protocol::SwarmRoutingStatus {
            resolved_execution: Some(target.clone()),
            actual_execution: Some(target.clone()),
            ..Default::default()
        });
        assert_eq!(member.durable_record().allowed_models, allowed);
        let dir = tempfile::TempDir::new().unwrap();
        let _env = test_env(&dir);
        persist_swarm_state("routing-test", None, Some("root"), &[member]);
        let loaded = load_runtime_state();
        let routing = loaded.members["child"].runtime.routing.as_ref().unwrap();
        assert_eq!(routing.resolved_execution.as_ref(), Some(&target));
        assert_eq!(routing.actual_execution, None);
        assert_eq!(
            loaded.members["child"].durable_record().allowed_models,
            allowed
        );
    }
}
