#[test]
fn schema_still_requires_action() {
    let schema = CommunicateTool::new().parameters_schema();
    assert_eq!(schema["required"], json!(["action"]));
}

#[test]
fn schema_advertises_model_and_effort_spawn_overrides() {
    let schema = CommunicateTool::new().parameters_schema();
    let props = schema["properties"]
        .as_object()
        .expect("swarm schema should have properties");

    assert_eq!(props["model"]["type"], json!("string"));
    assert!(
        props["model"]["description"]
            .as_str()
            .expect("model description")
            .contains("list_models"),
        "model param should point at the list_models action"
    );
    assert!(props.contains_key("effort"));
    assert_eq!(
        props["effort"]["enum"],
        json!(["none", "minimal", "low", "medium", "high", "xhigh", "max"])
    );
    assert!(
        schema["properties"]["action"]["enum"]
            .as_array()
            .expect("action enum")
            .contains(&json!("list_models"))
    );
}

#[test]
fn schema_requires_a_nonblank_label_for_spawn() {
    let schema = CommunicateTool::new().parameters_schema();
    assert_eq!(schema["properties"]["label"]["minLength"], json!(1));
    assert!(
        schema["properties"]["label"]["description"]
            .as_str()
            .expect("label description")
            .contains("Required for spawn")
    );
    assert!(
        schema["properties"]["action"]["description"]
            .as_str()
            .expect("action description")
            .contains("spawn requires label")
    );

    let branches = schema["anyOf"]
        .as_array()
        .expect("swarm schema should declare action-specific branches");
    let spawn_branch = branches
        .iter()
        .find(|branch| branch["properties"]["action"]["enum"] == json!(["spawn"]))
        .expect("spawn schema branch");
    assert_eq!(spawn_branch["required"], json!(["action", "label"]));

    let non_spawn_branch = branches
        .iter()
        .find(|branch| {
            branch["properties"]["action"]["enum"]
                .as_array()
                .is_some_and(|actions| !actions.contains(&json!("spawn")))
        })
        .expect("non-spawn schema branch");
    assert_eq!(non_spawn_branch["required"], json!(["action"]));
}

#[test]
fn schema_branches_only_require_properties_they_declare() {
    // Gemini rejects the entire request when a `required` entry names a property
    // the same object does not define, which made every tool-enabled Gemini call
    // fail on this tool's spawn branch (issue #655).
    let schema = CommunicateTool::new().parameters_schema();
    for branch in schema["anyOf"].as_array().expect("schema branches") {
        let declared = branch["properties"]
            .as_object()
            .expect("branch properties")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for required in branch["required"].as_array().expect("branch required") {
            let name = required.as_str().expect("required name");
            assert!(
                declared.iter().any(|known| known == name),
                "branch requires '{name}' without declaring it: {branch}"
            );
        }
    }
}

#[test]
fn spawn_label_validation_rejects_missing_or_blank_labels() {
    let missing: CommunicateInput =
        serde_json::from_value(json!({"action": "spawn"})).expect("spawn input");
    assert_eq!(
        missing
            .required_spawn_label()
            .expect_err("missing label must fail")
            .to_string(),
        "'label' is required for spawn action"
    );

    let blank: CommunicateInput = serde_json::from_value(json!({
        "action": "spawn",
        "label": "  \n\t "
    }))
    .expect("spawn input");
    assert_eq!(
        blank
            .required_spawn_label()
            .expect_err("blank label must fail")
            .to_string(),
        "'label' must not be blank for spawn action"
    );
}

#[test]
fn spawn_label_validation_trims_valid_labels() {
    let params: CommunicateInput = serde_json::from_value(json!({
        "action": "spawn",
        "label": "  api reviewer  "
    }))
    .expect("spawn input");
    assert_eq!(
        params.required_spawn_label().expect("valid label"),
        "api reviewer"
    );
}

#[tokio::test]
async fn spawn_execute_rejects_missing_label_before_sending_request() {
    let working_dir = tempfile::tempdir().expect("working dir");
    let error = CommunicateTool::new()
        .execute(
            json!({"action": "spawn", "prompt": "review the API"}),
            test_ctx("session-parent", working_dir.path()),
        )
        .await
        .expect_err("missing spawn label must fail locally");

    assert_eq!(error.to_string(), "'label' is required for spawn action");
}

#[test]
fn description_includes_swarm_prompt_guidance() {
    let tool = CommunicateTool::new();
    let description = tool.description();
    assert!(
        description.starts_with("Coordinate agents"),
        "description should lead with the short coordination summary"
    );
    assert!(
        description.contains("Swarm prompt"),
        "description should embed the swarm prompt section"
    );
}

#[test]
fn existing_tool_keeps_prompt_while_new_tool_loads_edit() {
    let project = tempfile::tempdir().unwrap();
    let prompt_dir = project.path().join(".jcode");
    std::fs::create_dir_all(&prompt_dir).unwrap();
    let prompt_path = prompt_dir.join("swarm-prompt.md");
    std::fs::write(&prompt_path, "first routing version").unwrap();

    let existing = CommunicateTool::new_for_working_dir(Some(project.path()));
    std::fs::write(&prompt_path, "second routing version").unwrap();
    let newly_created = CommunicateTool::new_for_working_dir(Some(project.path()));

    assert!(existing.description().contains("first routing version"));
    assert!(!existing.description().contains("second routing version"));
    assert!(
        newly_created
            .description()
            .contains("second routing version")
    );
}

#[test]
fn spawning_action_inputs_preserve_requested_model() {
    for action in [
        "spawn",
        "assign_task",
        "assign_next",
        "fill_slots",
        "run_plan",
    ] {
        for model in [
            "z-ai/glm-5.2:free",
            "openai-api:gpt-5.5",
            "inherit",
            "coordinator",
            "  ",
        ] {
            let input: CommunicateInput = serde_json::from_value(json!({
                "action": action,
                "label": "reviewer",
                "model": model
            }))
            .unwrap();
            assert_eq!(input.model.as_deref(), Some(model));
        }
    }
}

#[test]
fn spawning_action_inputs_allow_omitted_or_null_model() {
    for action in [
        "spawn",
        "assign_task",
        "assign_next",
        "fill_slots",
        "run_plan",
    ] {
        let without_model: CommunicateInput =
            serde_json::from_value(json!({"action": action, "label": "reviewer"})).unwrap();
        assert!(without_model.model.is_none());
        let null_model: CommunicateInput = serde_json::from_value(json!({
            "action": action,
            "label": "reviewer",
            "model": null
        }))
        .unwrap();
        assert!(null_model.model.is_none());
    }
}

#[test]
fn format_swarm_model_list_renders_routes_and_default() {
    let routes = vec![
        jcode_provider_core::ModelRoute {
            model: "gpt-5.5".to_string(),
            provider: "OpenAI".to_string(),
            api_method: "openai-api-key".to_string(),
            available: true,
            detail: "API key".to_string(),
            cheapness: None,
        },
        jcode_provider_core::ModelRoute {
            model: "claude-fable-5".to_string(),
            provider: "Anthropic".to_string(),
            api_method: "anthropic-api-key".to_string(),
            available: false,
            detail: String::new(),
            cheapness: None,
        },
    ];
    let output = format_swarm_model_list(
        Some("claude-fable-5"),
        Some("openai-api:gpt-5.5"),
        &routes,
        None,
    );
    assert!(output.contains("Current coordinator model: claude-fable-5"));
    assert!(output.contains("Configured agents.swarm_model default: openai-api:gpt-5.5"));
    assert!(output.contains("gpt-5.5 via OpenAI [openai-api-key] (API key)"));
    assert!(output.contains("claude-fable-5 via Anthropic [anthropic-api-key] [unavailable]"));
    assert!(output.contains("effort"));
}

#[test]
fn format_swarm_model_list_handles_empty_catalog() {
    let output = format_swarm_model_list(None, None, &[], None);
    assert!(output.contains("Current coordinator model: unknown"));
    assert!(output.contains("No agents.swarm_model default configured"));
    assert!(output.contains("unless model is passed"));
    assert!(output.contains("No model routes reported"));
}

#[test]
fn schema_advertises_supported_swarm_fields() {
    let schema = CommunicateTool::new().parameters_schema();
    let props = schema["properties"]
        .as_object()
        .expect("swarm schema should have properties");

    assert!(props.contains_key("action"));
    assert!(props.contains_key("key"));
    assert!(props.contains_key("value"));
    assert!(props.contains_key("message"));
    assert!(props.contains_key("to_session"));
    assert_eq!(
        props["to_session"]["description"],
        json!("Session ID or unique friendly name of one agent. Alias of target_session.")
    );
    assert!(props.contains_key("channel"));
    assert!(props.contains_key("proposer_session"));
    assert!(props.contains_key("reason"));
    assert!(props.contains_key("target_session"));
    assert_eq!(
        props["target_session"]["description"],
        json!("Session ID or unique friendly name for management actions. Alias of to_session.")
    );
    assert!(props.contains_key("role"));
    assert!(props.contains_key("prompt"));
    assert!(props.contains_key("working_dir"));
    assert!(props.contains_key("limit"));
    assert!(props.contains_key("task_id"));
    assert!(props.contains_key("spawn_if_needed"));
    assert!(props.contains_key("prefer_spawn"));
    assert!(props.contains_key("session_ids"));
    assert!(props.contains_key("mode"));
    assert_eq!(
        props["mode"]["enum"],
        json!(["all", "any", "deep", "light"]),
        "mode must advertise both task_graph and await_members values"
    );
    assert!(props.contains_key("target_status"));
    assert!(props.contains_key("timeout_minutes"));
    assert!(props.contains_key("concurrency_limit"));
    assert!(props.contains_key("wake"));
    assert!(props.contains_key("delivery"));
    assert!(props.contains_key("plan_items"));
    assert!(props.contains_key("initial_message"));
    assert!(props.contains_key("force"));
    assert!(props.contains_key("retain_agents"));
    assert!(props.contains_key("background"));
    assert!(
        props["background"]["description"]
            .as_str()
            .expect("background description")
            .contains("run_plan"),
        "background flag should document run_plan support"
    );
    assert!(props.contains_key("notify"));
    assert!(props.contains_key("status"));
    assert!(props.contains_key("validation"));
    assert!(props.contains_key("follow_up"));
    assert_eq!(
        props["delivery"]["enum"],
        json!(["notify", "interrupt", "wake"])
    );
    assert_eq!(
        props["plan_items"]["items"]["additionalProperties"],
        json!(true)
    );
    assert!(
        schema["properties"]["action"]["enum"]
            .as_array()
            .expect("action enum")
            .contains(&json!("status"))
    );
    assert!(
        schema["properties"]["action"]["enum"]
            .as_array()
            .expect("action enum")
            .contains(&json!("report"))
    );
    assert!(
        schema["properties"]["action"]["enum"]
            .as_array()
            .expect("action enum")
            .contains(&json!("plan_status"))
    );
    assert!(
        schema["properties"]["action"]["enum"]
            .as_array()
            .expect("action enum")
            .contains(&json!("start"))
    );
    assert!(
        schema["properties"]["action"]["enum"]
            .as_array()
            .expect("action enum")
            .contains(&json!("start_task"))
    );
    assert!(
        schema["properties"]["action"]["enum"]
            .as_array()
            .expect("action enum")
            .contains(&json!("assign_next"))
    );
    assert!(
        schema["properties"]["action"]["enum"]
            .as_array()
            .expect("action enum")
            .contains(&json!("fill_slots"))
    );
    assert!(
        schema["properties"]["action"]["enum"]
            .as_array()
            .expect("action enum")
            .contains(&json!("run_plan"))
    );
    assert!(
        schema["properties"]["action"]["enum"]
            .as_array()
            .expect("action enum")
            .contains(&json!("cleanup"))
    );
    assert!(
        schema["properties"]["action"]["enum"]
            .as_array()
            .expect("action enum")
            .contains(&json!("salvage"))
    );
}
