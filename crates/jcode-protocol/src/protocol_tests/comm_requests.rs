#[test]
fn test_comm_propose_plan_roundtrip() -> Result<()> {
    let req = Request::CommProposePlan {
        id: 42,
        session_id: "sess_a".to_string(),
        items: vec![PlanItem {
            content: "Refactor parser".to_string(),
            status: "pending".to_string(),
            priority: "high".to_string(),
            id: "p1".to_string(),
            subsystem: None,
            file_scope: Vec::new(),
            blocked_by: vec!["p0".to_string()],
            assigned_to: Some("sess_b".to_string()),
        }],
    };
    let json = serde_json::to_string(&req)?;
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 42);
    let Request::CommProposePlan { items, .. } = decoded else {
        return Err(anyhow!("wrong request type"));
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, "p1");
    Ok(())
}

#[test]
fn test_stdin_response_roundtrip() -> Result<()> {
    let req = Request::StdinResponse {
        id: 99,
        request_id: "stdin-call_abc-1".to_string(),
        input: "my_password".to_string(),
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"stdin_response\""));
    assert!(json.contains("\"request_id\":\"stdin-call_abc-1\""));
    assert!(json.contains("\"input\":\"my_password\""));

    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 99);
    let Request::StdinResponse {
        request_id, input, ..
    } = decoded
    else {
        return Err(anyhow!("expected StdinResponse"));
    };
    assert_eq!(request_id, "stdin-call_abc-1");
    assert_eq!(input, "my_password");
    Ok(())
}

#[test]
fn test_stdin_response_deserialize_from_json() -> Result<()> {
    let json = r#"{"type":"stdin_response","id":5,"request_id":"req-42","input":"hello world"}"#;
    let decoded = parse_request_json(json)?;
    assert_eq!(decoded.id(), 5);
    let Request::StdinResponse {
        request_id, input, ..
    } = decoded
    else {
        return Err(anyhow!("expected StdinResponse"));
    };
    assert_eq!(request_id, "req-42");
    assert_eq!(input, "hello world");
    Ok(())
}

#[test]
fn test_stdin_request_event_roundtrip() -> Result<()> {
    let event = ServerEvent::StdinRequest {
        request_id: "stdin-xyz-1".to_string(),
        prompt: "Password: ".to_string(),
        is_password: true,
        tool_call_id: "call_abc".to_string(),
    };
    let json = encode_event(&event);
    assert!(json.contains("\"type\":\"stdin_request\""));
    assert!(json.contains("\"is_password\":true"));

    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::StdinRequest {
        request_id,
        prompt,
        is_password,
        tool_call_id,
    } = decoded
    else {
        return Err(anyhow!("expected StdinRequest"));
    };
    assert_eq!(request_id, "stdin-xyz-1");
    assert_eq!(prompt, "Password: ");
    assert!(is_password);
    assert_eq!(tool_call_id, "call_abc");
    Ok(())
}

#[test]
fn test_stdin_request_event_defaults() -> Result<()> {
    // is_password defaults to false when not present
    let json = r#"{"type":"stdin_request","request_id":"r1","prompt":"","tool_call_id":"tc1"}"#;
    let decoded = parse_event_json(json)?;
    let ServerEvent::StdinRequest { is_password, .. } = decoded else {
        return Err(anyhow!("expected StdinRequest"));
    };
    assert!(!is_password, "is_password should default to false");
    Ok(())
}

#[test]
fn test_comm_await_members_roundtrip() -> Result<()> {
    let req = Request::CommAwaitMembers {
        id: 55,
        session_id: "sess_waiter".to_string(),
        target_status: vec!["completed".to_string(), "stopped".to_string()],
        session_ids: vec!["sess_a".to_string(), "sess_b".to_string()],
        mode: Some("any".to_string()),
        timeout_secs: Some(120),
        background: false,
        notify: false,
        wake: false,
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"comm_await_members\""));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 55);
    let Request::CommAwaitMembers {
        session_id,
        target_status,
        session_ids,
        mode,
        timeout_secs,
        background,
        notify,
        wake,
        ..
    } = decoded
    else {
        return Err(anyhow!("expected CommAwaitMembers"));
    };
    assert_eq!(session_id, "sess_waiter");
    assert_eq!(target_status, vec!["completed", "stopped"]);
    assert_eq!(session_ids, vec!["sess_a", "sess_b"]);
    assert_eq!(mode.as_deref(), Some("any"));
    assert_eq!(timeout_secs, Some(120));
    assert!(!background);
    assert!(!notify);
    assert!(!wake);
    Ok(())
}

#[test]
fn test_comm_await_members_defaults() -> Result<()> {
    let json =
        r#"{"type":"comm_await_members","id":1,"session_id":"s1","target_status":["completed"]}"#;
    let decoded = parse_request_json(json)?;
    let Request::CommAwaitMembers {
        session_ids,
        mode,
        timeout_secs,
        background,
        notify,
        wake,
        ..
    } = decoded
    else {
        return Err(anyhow!("expected CommAwaitMembers"));
    };
    assert!(
        session_ids.is_empty(),
        "session_ids should default to empty"
    );
    assert_eq!(mode, None, "mode should default to None");
    assert_eq!(timeout_secs, None, "timeout_secs should default to None");
    assert!(background, "background should default to true");
    assert!(notify, "notify should default to true");
    assert!(wake, "wake should default to true");
    Ok(())
}

#[test]
fn test_comm_report_roundtrip() -> Result<()> {
    let req = Request::CommReport {
        id: 57,
        session_id: "sess_worker".to_string(),
        status: Some("ready".to_string()),
        message: "Implemented report action.".to_string(),
        validation: Some("Focused tests passed.".to_string()),
        follow_up: Some("None.".to_string()),
        tldr: None,
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"comm_report\""));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 57);
    let Request::CommReport {
        session_id,
        status,
        message,
        validation,
        follow_up,
        ..
    } = decoded
    else {
        return Err(anyhow!("expected CommReport"));
    };
    assert_eq!(session_id, "sess_worker");
    assert_eq!(status.as_deref(), Some("ready"));
    assert_eq!(message, "Implemented report action.");
    assert_eq!(validation.as_deref(), Some("Focused tests passed."));
    assert_eq!(follow_up.as_deref(), Some("None."));
    Ok(())
}

#[test]
fn test_comm_report_response_roundtrip() -> Result<()> {
    let event = ServerEvent::CommReportResponse {
        id: 57,
        status: "ready".to_string(),
        message: "Report recorded.".to_string(),
    };
    let json = encode_event(&event);
    assert!(json.contains("\"type\":\"comm_report_response\""));
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::CommReportResponse {
        id,
        status,
        message,
    } = decoded
    else {
        return Err(anyhow!("expected CommReportResponse"));
    };
    assert_eq!(id, 57);
    assert_eq!(status, "ready");
    assert_eq!(message, "Report recorded.");
    Ok(())
}

#[test]
fn test_comm_await_members_response_roundtrip() -> Result<()> {
    let event = ServerEvent::CommAwaitMembersResponse {
        id: 55,
        completed: true,
        members: vec![
            AwaitedMemberStatus {
                session_id: "sess_a".to_string(),
                friendly_name: Some("fox".to_string()),
                status: "completed".to_string(),
                done: true,
                completion_report: None,
            },
            AwaitedMemberStatus {
                session_id: "sess_b".to_string(),
                friendly_name: Some("wolf".to_string()),
                status: "stopped".to_string(),
                done: true,
                completion_report: None,
            },
        ],
        summary: "All 2 members are done: fox, wolf".to_string(),
        background_started: false,
    };
    let json = encode_event(&event);
    assert!(json.contains("\"type\":\"comm_await_members_response\""));
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::CommAwaitMembersResponse {
        id,
        completed,
        members,
        summary,
        ..
    } = decoded
    else {
        return Err(anyhow!("expected CommAwaitMembersResponse"));
    };
    assert_eq!(id, 55);
    assert!(completed);
    assert_eq!(members.len(), 2);
    assert_eq!(members[0].friendly_name.as_deref(), Some("fox"));
    assert!(members[0].done);
    assert_eq!(members[1].status, "stopped");
    assert!(summary.contains("fox"));
    Ok(())
}

#[test]
fn test_comm_task_control_roundtrip() -> Result<()> {
    let req = Request::CommTaskControl {
        id: 58,
        session_id: "sess_coord".to_string(),
        action: "salvage".to_string(),
        task_id: "task_42".to_string(),
        target_session: Some("sess_replacement".to_string()),
        message: Some("Recover partial progress first.".to_string()),
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"comm_task_control\""));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 58);
    let Request::CommTaskControl {
        session_id,
        action,
        task_id,
        target_session,
        message,
        ..
    } = decoded
    else {
        return Err(anyhow!("expected CommTaskControl"));
    };
    assert_eq!(session_id, "sess_coord");
    assert_eq!(action, "salvage");
    assert_eq!(task_id, "task_42");
    assert_eq!(target_session.as_deref(), Some("sess_replacement"));
    assert_eq!(message.as_deref(), Some("Recover partial progress first."));
    Ok(())
}

#[test]
fn test_comm_assign_task_roundtrip_without_explicit_task_id() -> Result<()> {
    let req = Request::CommAssignTask {
        model: None,
        effort: None,
        id: 57,
        session_id: "sess_coord".to_string(),
        target_session: None,
        task_id: None,
        message: Some("Take the next highest-priority runnable task.".to_string()),
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"comm_assign_task\""));
    assert!(!json.contains("\"task_id\""));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 57);
    let Request::CommAssignTask {
        session_id,
        target_session,
        task_id,
        message,
        ..
    } = decoded
    else {
        return Err(anyhow!("expected CommAssignTask"));
    };
    assert_eq!(session_id, "sess_coord");
    assert_eq!(target_session, None);
    assert_eq!(task_id, None);
    assert_eq!(
        message.as_deref(),
        Some("Take the next highest-priority runnable task.")
    );
    Ok(())
}

#[test]
fn test_comm_assign_task_response_roundtrip() -> Result<()> {
    let event = ServerEvent::CommAssignTaskResponse {
        routing: None,
        id: 60,
        task_id: "task-7".to_string(),
        target_session: "sess_worker".to_string(),
    };
    let json = encode_event(&event);
    assert!(json.contains("\"type\":\"comm_assign_task_response\""));
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::CommAssignTaskResponse {
        id,
        task_id,
        target_session,
        ..
    } = decoded
    else {
        return Err(anyhow!("expected CommAssignTaskResponse"));
    };
    assert_eq!(id, 60);
    assert_eq!(task_id, "task-7");
    assert_eq!(target_session, "sess_worker");
    Ok(())
}

#[test]
fn test_comm_assign_next_roundtrip() -> Result<()> {
    let req = Request::CommAssignNext {
        allowed_models: None,
        id: 60,
        session_id: "sess_coord".to_string(),
        target_session: Some("sess_worker".to_string()),
        working_dir: Some("/tmp/project".to_string()),
        prefer_spawn: Some(true),
        spawn_if_needed: Some(true),
        message: Some("Take the next runnable task.".to_string()),
        model: Some("openai-api:gpt-5.5".to_string()),
        effort: Some("low".to_string()),
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"comm_assign_next\""));
    assert!(json.contains("\"model\":\"openai-api:gpt-5.5\""));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 60);
    let Request::CommAssignNext {
        session_id,
        target_session,
        working_dir,
        prefer_spawn,
        spawn_if_needed,
        message,
        model,
        effort,
        ..
    } = decoded
    else {
        return Err(anyhow!("expected CommAssignNext"));
    };
    assert_eq!(session_id, "sess_coord");
    assert_eq!(target_session.as_deref(), Some("sess_worker"));
    assert_eq!(working_dir.as_deref(), Some("/tmp/project"));
    assert_eq!(prefer_spawn, Some(true));
    assert_eq!(spawn_if_needed, Some(true));
    assert_eq!(message.as_deref(), Some("Take the next runnable task."));
    assert_eq!(model.as_deref(), Some("openai-api:gpt-5.5"));
    assert_eq!(effort.as_deref(), Some("low"));
    Ok(())
}

#[test]
fn test_comm_stop_roundtrip_with_force() -> Result<()> {
    let req = Request::CommStop {
        id: 61,
        session_id: "sess_coord".to_string(),
        target_session: "sess_worker".to_string(),
        force: Some(true),
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"comm_stop\""));
    assert!(json.contains("\"force\":true"));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 61);
    let Request::CommStop {
        session_id,
        target_session,
        force,
        ..
    } = decoded
    else {
        return Err(anyhow!("expected CommStop"));
    };
    assert_eq!(session_id, "sess_coord");
    assert_eq!(target_session, "sess_worker");
    assert_eq!(force, Some(true));
    Ok(())
}

#[test]
fn test_comm_spawn_roundtrip_with_optional_nonce() -> Result<()> {
    let req = Request::CommSpawn {
        allowed_models: None,
        id: 59,
        session_id: "sess_coord".to_string(),
        working_dir: Some("/tmp/project".to_string()),
        initial_message: Some("Start here".to_string()),
        request_nonce: Some("planner-fresh-123".to_string()),
        spawn_mode: Some("headless".to_string()),
        model: Some("openai-api:gpt-5.5".to_string()),
        effort: Some("low".to_string()),
        label: Some("review auth flow".to_string()),
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"comm_spawn\""));
    assert!(json.contains("\"request_nonce\":\"planner-fresh-123\""));
    assert!(json.contains("\"spawn_mode\":\"headless\""));
    assert!(json.contains("\"effort\":\"low\""));
    assert!(json.contains("\"label\":\"review auth flow\""));
    assert!(json.contains("\"model\":\"openai-api:gpt-5.5\""));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 59);
    let Request::CommSpawn {
        session_id,
        working_dir,
        initial_message,
        request_nonce,
        spawn_mode,
        model,
        effort,
        label,
        ..
    } = decoded
    else {
        return Err(anyhow!("expected CommSpawn"));
    };
    assert_eq!(session_id, "sess_coord");
    assert_eq!(working_dir.as_deref(), Some("/tmp/project"));
    assert_eq!(initial_message.as_deref(), Some("Start here"));
    assert_eq!(request_nonce.as_deref(), Some("planner-fresh-123"));
    assert_eq!(spawn_mode.as_deref(), Some("headless"));
    assert_eq!(model.as_deref(), Some("openai-api:gpt-5.5"));
    assert_eq!(effort.as_deref(), Some("low"));
    assert_eq!(label.as_deref(), Some("review auth flow"));
    Ok(())
}

#[test]
fn test_comm_spawn_and_assign_next_decode_model_without_effort() -> Result<()> {
    for request_type in ["comm_spawn", "comm_assign_next"] {
        let json = serde_json::json!({
            "type": request_type,
            "id": 60,
            "session_id": "sess_coord",
            "model": "gpt-5.5"
        });
        let decoded = parse_request_json(&json.to_string())?;
        match decoded {
            Request::CommSpawn {
                model,
                effort,
                label,
                ..
            } => {
                assert_eq!(model.as_deref(), Some("gpt-5.5"));
                assert_eq!(effort, None);
                assert_eq!(label, None);
            }
            Request::CommAssignNext { model, effort, .. } => {
                assert_eq!(model.as_deref(), Some("gpt-5.5"));
                assert_eq!(effort, None);
            }
            _ => return Err(anyhow!("expected spawn or assign_next")),
        }
    }
    Ok(())
}

#[test]
fn test_comm_spawn_and_assign_next_roundtrip_omitted_or_null_model() -> Result<()> {
    for request_type in ["comm_spawn", "comm_assign_next"] {
        for explicit_null in [false, true] {
            // Older clients omit the optional field. Explicit null must also work.
            let mut json = serde_json::json!({
                "type": request_type,
                "id": 60,
                "session_id": "sess_coord"
            });
            if explicit_null {
                json["model"] = serde_json::Value::Null;
            }
            let decoded = parse_request_json(&json.to_string())?;
            let encoded = serde_json::to_string(&decoded)?;
            let roundtripped = parse_request_json(&encoded)?;
            for request in [decoded, roundtripped] {
                assert_eq!(request.id(), 60);
                match request {
                    Request::CommSpawn {
                        model,
                        effort,
                        label,
                        ..
                    } => {
                        assert_eq!(model, None);
                        assert_eq!(effort, None);
                        assert_eq!(label, None);
                    }
                    Request::CommAssignNext { model, effort, .. } => {
                        assert_eq!(model, None);
                        assert_eq!(effort, None);
                    }
                    _ => return Err(anyhow!("expected spawn or assign_next")),
                }
            }
        }
    }
    Ok(())
}

#[test]
fn test_comm_list_models_roundtrip() -> Result<()> {
    let req = Request::CommListModels {
        id: 61,
        session_id: "sess_coord".to_string(),
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"comm_list_models\""));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 61);
    assert!(decoded.is_lightweight_control_request());
    let Request::CommListModels { session_id, .. } = decoded else {
        return Err(anyhow!("expected CommListModels"));
    };
    assert_eq!(session_id, "sess_coord");
    Ok(())
}

#[test]
fn test_reload_force_defaults_true_for_legacy_clients() -> Result<()> {
    // Old clients (and the desktop Swift enum, which has no reload case) send a
    // reload request with no `force` field. It must default to true so their
    // behavior stays unconditional, matching the pre-#291 protocol.
    let json = r#"{"type":"reload","id":7}"#;
    let decoded = parse_request_json(json)?;
    let Request::Reload { id, force } = decoded else {
        return Err(anyhow!("expected Reload"));
    };
    assert_eq!(id, 7);
    assert!(force, "missing force must default to true");
    Ok(())
}

#[test]
fn test_reload_force_roundtrip() -> Result<()> {
    for force in [false, true] {
        let req = Request::Reload { id: 9, force };
        let json = serde_json::to_string(&req)?;
        assert!(json.contains("\"type\":\"reload\""));
        let decoded = parse_request_json(&json)?;
        let Request::Reload {
            id,
            force: decoded_force,
        } = decoded
        else {
            return Err(anyhow!("expected Reload"));
        };
        assert_eq!(id, 9);
        assert_eq!(decoded_force, force);
    }
    Ok(())
}

#[test]
fn test_comm_routing_wire_requests_preserve_optional_constraints() -> Result<()> {
    for kind in ["comm_spawn", "comm_assign_next"] {
        for allowed in [
            None,
            Some(serde_json::json!([])),
            Some(serde_json::json!(["openai-oauth:gpt-6-astra"])),
        ] {
            let mut input = serde_json::json!({"type":kind, "id":1, "session_id":"root", "model":"openai-oauth:gpt-6-astra", "effort":"high"});
            if let Some(value) = &allowed {
                input["allowed_models"] = value.clone();
            }
            let request: Request = serde_json::from_value(input)?;
            let output = serde_json::to_value(request)?;
            assert_eq!(output["model"], "openai-oauth:gpt-6-astra");
            assert_eq!(output["effort"], "high");
            assert_eq!(output.get("allowed_models").cloned(), allowed);
        }
        let old: Request =
            serde_json::from_value(serde_json::json!({"type":kind,"id":1,"session_id":"root"}))?;
        let output = serde_json::to_value(old)?;
        for field in ["model", "effort", "allowed_models"] {
            assert!(output.get(field).is_none());
        }
    }
    let node: TaskGraphNodeSpec = serde_json::from_value(
        serde_json::json!({"id":"r","content":"work","model":"claude-oauth:claude-opus-5","effort":"medium"}),
    )?;
    let output = serde_json::to_value(node)?;
    assert_eq!(output["model"], "claude-oauth:claude-opus-5");
    assert_eq!(output["effort"], "medium");
    let old: TaskGraphNodeSpec =
        serde_json::from_value(serde_json::json!({"id":"old","content":"work"}))?;
    assert!(serde_json::to_value(old)?.get("model").is_none());
    Ok(())
}

#[test]
fn test_comm_routing_status_roundtrips_actual_identity_and_policy() -> Result<()> {
    let routing = serde_json::json!({
        "requested_model":"openai-oauth:gpt-6-astra", "requested_effort":"high",
        "resolved_execution":{"model":"gpt-6-astra","provider_key":"openai-oauth","api_method":"OpenAI OAuth","provider_label":"OpenAI","effort":"high"},
        "actual_execution":{"model":"gpt-6-astra","provider_key":"openai-oauth","api_method":"OpenAI OAuth","provider_label":"OpenAI","effort":"high"},
        "selection_enabled":true,"allowed_models":[]
    });
    for input in [
        serde_json::json!({"type":"comm_spawn_response","id":1,"session_id":"root","new_session_id":"child","routing":routing}),
        serde_json::json!({"type":"comm_assign_task_response","id":1,"task_id":"r","target_session":"child","routing":routing}),
        serde_json::json!({"type":"comm_list_models_response","id":1,"routing":routing}),
    ] {
        let event: ServerEvent = serde_json::from_value(input.clone())?;
        assert_eq!(serde_json::to_value(event)?["routing"], routing);
        let mut old = input;
        old.as_object_mut().unwrap().remove("routing");
        let event: ServerEvent = serde_json::from_value(old)?;
        assert!(serde_json::to_value(event)?.get("routing").is_none());
    }
    let runtime: SwarmMemberRuntime =
        serde_json::from_value(serde_json::json!({"routing":routing}))?;
    assert_eq!(serde_json::to_value(&runtime)?["routing"], routing);
    assert!(!runtime.is_empty());
    Ok(())
}

#[test]
fn test_comm_routing_plan_status_reports_durable_requests_and_execution() -> Result<()> {
    let mut plan = VersionedPlan::default();
    let metadata: jcode_plan::NodeMeta = serde_json::from_value(serde_json::json!({
        "model":"claude-oauth:claude-opus-5", "effort":"medium",
        "resolved_execution":{"model":"claude-opus-5","provider_key":"claude-oauth","api_method":"Claude OAuth","provider_label":"Anthropic","effort":"medium"}
    }))?;
    plan.node_meta.insert("r".into(), metadata.clone());
    let status = PlanGraphStatus::from_versioned_plan("swarm", &plan, None, vec![]);
    let output = serde_json::to_value(status)?;
    assert!(
        output["node_routing"]["r"]
            .get("actual_execution")
            .is_none(),
        "persisted selection alone is not proof of initialized identity"
    );
    assert_eq!(
        output["node_routing"]["r"]["requested_model"],
        "claude-oauth:claude-opus-5"
    );
    assert_eq!(output["node_routing"]["r"]["requested_effort"], "medium");
    assert_eq!(
        output["node_routing"]["r"]["resolved_execution"],
        serde_json::to_value(metadata)?["resolved_execution"]
    );
    Ok(())
}

#[test]
fn test_comm_routing_wire_to_plan_bridge_keeps_requested_constraints() -> Result<()> {
    let wire: TaskGraphNodeSpec = serde_json::from_value(serde_json::json!({
        "id":"r", "content":"work", "model":"openai-oauth:gpt-6-astra", "effort":"high",
        "resolved_execution":{"model":"untrusted"}
    }))?;
    // Resolved identity is output-only, never admitted from untrusted node specs.
    assert!(
        serde_json::to_value(&wire)?
            .get("resolved_execution")
            .is_none()
    );
    let spec = jcode_plan::dag::NodeSpec {
        id: Some(wire.id),
        content: wire.content,
        kind: jcode_plan::dag::NodeKind::Explore,
        depends_on: wire.depends_on,
        priority: wire.priority,
        model: wire.model,
        effort: wire.effort,
    };
    let mut graph = jcode_plan::dag::TaskGraph::new(jcode_plan::dag::Mode::Light);
    jcode_plan::dag::seed(&mut graph, vec![spec])?;
    let mut plan = VersionedPlan::default();
    jcode_plan::bridge::apply_task_graph(&mut plan, &graph);
    plan.node_meta = serde_json::from_slice(&serde_json::to_vec(&plan.node_meta)?)?;
    let lifted = jcode_plan::bridge::to_task_graph(&plan);
    assert_eq!(lifted.get("r"), graph.get("r"));
    assert_eq!(
        plan.node_meta["r"].model.as_deref(),
        Some("openai-oauth:gpt-6-astra")
    );
    assert_eq!(plan.node_meta["r"].effort.as_deref(), Some("high"));
    assert!(plan.node_meta["r"].resolved_execution.is_none());
    Ok(())
}
