//! Replay and routing metadata compatibility for durable task graphs.

use super::*;

#[test]
fn seed_replay_is_idempotent_and_preserves_runtime_state() {
    let original = spec("a", NodeKind::Explore).depends_on(["upstream"]);
    let mut g = dag(
        Mode::Light,
        vec![spec("upstream", NodeKind::Explore), original.clone()],
    );
    dispatch(&mut g, "upstream", "worker");
    let before = g.clone();

    seed(&mut g, vec![original]).unwrap();

    assert_eq!(g, before, "an identical replay must be a complete no-op");
}

#[test]
fn seed_replay_accepts_dependency_order_and_duplicate_differences() {
    let mut g = dag(
        Mode::Light,
        vec![
            spec("a", NodeKind::Explore),
            spec("b", NodeKind::Explore),
            spec("join", NodeKind::Synthesize).depends_on(["a", "b"]),
        ],
    );
    let before = g.clone();

    seed(
        &mut g,
        vec![spec("join", NodeKind::Synthesize).depends_on(["b", "a", "a"])],
    )
    .unwrap();

    assert_eq!(g, before);
}

#[test]
fn partial_seed_replay_adds_only_new_nodes() {
    let existing = spec("a", NodeKind::Explore);
    let mut g = dag(Mode::Deep, vec![existing.clone()]);

    seed(
        &mut g,
        vec![existing, spec("b", NodeKind::Implement).depends_on(["a"])],
    )
    .unwrap();

    assert_eq!(g.nodes().iter().filter(|node| node.id == "a").count(), 1);
    assert_eq!(g.nodes().iter().filter(|node| node.id == "b").count(), 1);
    let root_gate = g.get("plan::gate").expect("root gate");
    assert!(root_gate.depends_on.contains(&"a".to_string()));
    assert!(root_gate.depends_on.contains(&"b".to_string()));
}

#[test]
fn seed_replay_rejects_changed_existing_definition() {
    let mut g = dag(Mode::Light, vec![spec("a", NodeKind::Explore)]);
    let before = g.clone();
    let mut changed = spec("a", NodeKind::Explore);
    changed.priority = 7;

    let err = seed(&mut g, vec![changed]).unwrap_err();

    assert_eq!(err, DagError::DuplicateNode("a".into()));
    assert_eq!(g, before, "a conflict must not partially mutate the graph");
}

fn routing_spec(id: &str) -> NodeSpec {
    serde_json::from_value(serde_json::json!({
        "id": id, "content": "route work", "kind": "Implement",
        "model": "openai-oauth:gpt-6-astra", "effort": "high"
    }))
    .unwrap()
}

#[test]
fn routing_seed_preserves_requests_and_rejects_conflicting_replays() {
    let mut graph = TaskGraph::new(Mode::Light);
    let original = routing_spec("r");
    seed(&mut graph, vec![original.clone()]).unwrap();
    let node = serde_json::to_value(graph.get("r").unwrap()).unwrap();
    assert_eq!(node["model"], "openai-oauth:gpt-6-astra");
    assert_eq!(node["effort"], "high");
    for field in ["model", "effort"] {
        let mut changed = serde_json::to_value(&original).unwrap();
        changed[field] = "different".into();
        let changed: NodeSpec = serde_json::from_value(changed).unwrap();
        assert!(seed(&mut graph, vec![changed.clone()]).is_err());
        let mut empty = TaskGraph::new(Mode::Light);
        assert!(seed(&mut empty, vec![original.clone(), changed]).is_err());
        assert!(empty.nodes().is_empty());
    }
}

#[test]
fn routing_blank_explicit_selectors_are_rejected_atomically() {
    for field in ["model", "effort"] {
        let mut value = serde_json::to_value(routing_spec("r")).unwrap();
        value[field] = "  ".into();
        let mut graph = TaskGraph::new(Mode::Light);
        let error = seed(&mut graph, vec![serde_json::from_value(value).unwrap()]).unwrap_err();
        assert!(error.to_string().contains(field));
        assert!(error.to_string().contains("omit"));
        assert!(graph.nodes().is_empty());
    }
}

#[test]
fn routing_old_specs_and_graphs_remain_unconstrained() {
    let node: NodeSpec = serde_json::from_value(serde_json::json!({
        "id":"old", "content":"legacy", "kind":"Explore"
    }))
    .unwrap();
    let mut graph = TaskGraph::new(Mode::Light);
    seed(&mut graph, vec![node]).unwrap();
    let json = serde_json::to_value(&graph).unwrap();
    let restored: TaskGraph = serde_json::from_value(json).unwrap();
    let value = serde_json::to_value(restored.get("old").unwrap()).unwrap();
    for field in ["model", "effort", "resolved_execution"] {
        assert!(value.get(field).is_none());
    }
}

#[test]
fn routing_expand_preserves_parent_execution_and_independent_child_choices() {
    let original = routing_spec("parent");
    let mut graph = dag(Mode::Deep, vec![original.clone()]);
    let execution = serde_json::json!({"model":"gpt-6-astra","provider_key":"openai-oauth","api_method":"OpenAI OAuth","provider_label":"OpenAI","effort":"high"});
    let mut parent = serde_json::to_value(graph.get("parent").unwrap()).unwrap();
    parent["resolved_execution"] = execution.clone();
    parent["owner"] = "planner".into();
    parent["status"] = "Running".into();
    *graph.get_mut("parent").unwrap() = serde_json::from_value(parent).unwrap();
    let expanded = expand_node(
        &mut graph,
        "parent",
        "planner",
        vec![
            routing_spec("child"),
            spec("default-child", NodeKind::Explore),
        ],
    )
    .unwrap();
    let parent = serde_json::to_value(graph.get("parent").unwrap()).unwrap();
    assert_eq!(parent["resolved_execution"], execution);
    assert_eq!(parent["model"], "openai-oauth:gpt-6-astra");
    seed(&mut graph, vec![original]).unwrap();
    let child = serde_json::to_value(graph.get("child").unwrap()).unwrap();
    assert_eq!(child["effort"], "high");
    assert!(child.get("resolved_execution").is_none());
    for node in graph
        .nodes()
        .iter()
        .filter(|n| n.is_gate || n.id == "default-child")
    {
        let value = serde_json::to_value(node).unwrap();
        for field in ["model", "effort", "resolved_execution"] {
            assert!(value.get(field).is_none());
        }
    }
    let gate_id = expanded.gate_id.unwrap();
    graph.get_mut(&gate_id).unwrap().owner = Some("reviewer".into());
    graph.get_mut(&gate_id).unwrap().status = NodeStatus::Running;
    inject_from_gate(&mut graph, &gate_id, "reviewer", vec![routing_spec("fix")]).unwrap();
    let fix = serde_json::to_value(graph.get("fix").unwrap()).unwrap();
    assert_eq!(fix["model"], "openai-oauth:gpt-6-astra");
    assert!(fix.get("resolved_execution").is_none());
}

#[test]
fn routing_blank_expand_and_inject_leave_existing_graph_unchanged() {
    for field in ["model", "effort"] {
        let mut graph = dag(Mode::Deep, vec![routing_spec("parent")]);
        graph.get_mut("parent").unwrap().owner = Some("planner".into());
        graph.get_mut("parent").unwrap().status = NodeStatus::Running;
        let mut value = serde_json::to_value(routing_spec("child")).unwrap();
        value[field] = "  ".into();
        let invalid: NodeSpec = serde_json::from_value(value).unwrap();
        let before = serde_json::to_value(&graph).unwrap();
        assert!(matches!(
            expand_node(&mut graph, "parent", "planner", vec![invalid.clone()]),
            Err(DagError::InvalidSelector { .. })
        ));
        assert_eq!(serde_json::to_value(&graph).unwrap(), before);
        let gate = graph
            .nodes()
            .iter()
            .find(|node| node.is_gate)
            .unwrap()
            .id
            .clone();
        graph.get_mut(&gate).unwrap().owner = Some("reviewer".into());
        graph.get_mut(&gate).unwrap().status = NodeStatus::Running;
        let before = serde_json::to_value(&graph).unwrap();
        assert!(matches!(
            inject_from_gate(&mut graph, &gate, "reviewer", vec![invalid]),
            Err(DagError::InvalidSelector { .. })
        ));
        assert_eq!(serde_json::to_value(&graph).unwrap(), before);
    }
}
