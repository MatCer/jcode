use super::{DEFAULT_SWARM_PROMPT, SWARM_ROUTING_CONTRACT, load_swarm_prompt};

#[test]
fn swarm_prompt_read_failures_preserve_fallback_and_runtime_contract() {
    let _lock = crate::storage::lock_test_env();
    struct RestoreHome(Option<std::ffi::OsString>);
    impl Drop for RestoreHome {
        fn drop(&mut self) {
            match &self.0 {
                Some(previous) => crate::env::set_var("JCODE_HOME", previous),
                None => crate::env::remove_var("JCODE_HOME"),
            }
        }
    }
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let _restore = RestoreHome(std::env::var_os("JCODE_HOME"));
    crate::env::set_var("JCODE_HOME", home.path());
    std::fs::create_dir(project.path().join(".jcode")).unwrap();
    let global = home.path().join("swarm-prompt.md");
    let local = project.path().join(".jcode/swarm-prompt.md");
    std::fs::write(&global, "global guidance").unwrap();

    // Invalid UTF-8 is a deterministic read error even when tests run as root.
    std::fs::write(&local, [0xff]).unwrap();
    assert_eq!(
        load_swarm_prompt(Some(project.path())),
        format!("{SWARM_ROUTING_CONTRACT}\n\nglobal guidance")
    );

    // A malformed global override still leaves the built-in guidance available.
    std::fs::write(&global, [0xff]).unwrap();
    assert_eq!(
        load_swarm_prompt(Some(project.path())),
        format!(
            "{SWARM_ROUTING_CONTRACT}\n\n{}",
            DEFAULT_SWARM_PROMPT.trim()
        )
    );

    // A valid project override remains authoritative over an unreadable global one.
    std::fs::write(&local, "  project guidance\n").unwrap();
    assert_eq!(
        load_swarm_prompt(Some(project.path())),
        format!("{SWARM_ROUTING_CONTRACT}\n\nproject guidance")
    );
}
