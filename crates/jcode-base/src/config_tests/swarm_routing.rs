//! Config-file and environment compatibility for swarm execution defaults.

use super::*;

#[test]
fn test_env_override_swarm_model() {
    let _guard = crate::storage::lock_test_env();
    let prev = std::env::var_os("JCODE_SWARM_MODEL");
    crate::env::set_var("JCODE_SWARM_MODEL", "claude-opus-4-6");

    let mut cfg = Config::default();
    cfg.apply_env_overrides();

    assert_eq!(cfg.agents.swarm_model.as_deref(), Some("claude-opus-4-6"));

    // Empty value clears the override back to "inherit".
    crate::env::set_var("JCODE_SWARM_MODEL", "  ");
    let mut cfg = Config::default();
    cfg.agents.swarm_model = Some("preset".to_string());
    cfg.apply_env_overrides();
    assert_eq!(cfg.agents.swarm_model, None);

    restore_env_var("JCODE_SWARM_MODEL", prev);
}

#[test]
fn swarm_effort_parses_from_toml_and_env_override() {
    let _guard = crate::storage::lock_test_env();
    let prev = std::env::var_os("JCODE_SWARM_EFFORT");
    restore_env_var("JCODE_SWARM_EFFORT", None);

    // Public config-file interface (#1165).
    let cfg: Config =
        toml::from_str("[agents]\nswarm_model = \"claude-opus-5\"\nswarm_effort = \"medium\"\n")
            .expect("config with swarm_effort parses");
    assert_eq!(cfg.agents.swarm_effort.as_deref(), Some("medium"));
    assert_eq!(Config::default().agents.swarm_effort, None);

    crate::env::set_var("JCODE_SWARM_EFFORT", "low");
    let mut cfg = Config::default();
    cfg.apply_env_overrides();
    assert_eq!(cfg.agents.swarm_effort.as_deref(), Some("low"));

    crate::env::set_var("JCODE_SWARM_EFFORT", " ");
    let mut cfg = Config::default();
    cfg.agents.swarm_effort = Some("preset".to_string());
    cfg.apply_env_overrides();
    assert_eq!(cfg.agents.swarm_effort, None);

    restore_env_var("JCODE_SWARM_EFFORT", prev);
}

#[test]
fn swarm_routing_config_toml_defaults_and_empty_policy() {
    let old: Config = toml::from_str("[agents]\nswarm_model = \"inherit\"\n").unwrap();
    let value = serde_json::to_value(&old.agents).unwrap();
    assert_eq!(value["swarm_model_selection"], false);
    let empty: Config =
        toml::from_str("[agents]\nswarm_model_selection = true\nswarm_allowed_models = []\n")
            .unwrap();
    let value = serde_json::to_value(&empty.agents).unwrap();
    assert_eq!(value["swarm_model_selection"], true);
    assert_eq!(value["swarm_allowed_models"], serde_json::json!([]));
}
