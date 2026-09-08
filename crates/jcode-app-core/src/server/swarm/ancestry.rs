//! Trusted and best-effort views of the existing swarm parent chain.

use super::SwarmMember;
use std::collections::{HashMap, HashSet};

/// Return the trusted chain, including `session_id` first and the root last.
/// Missing members, cycles, and cross-swarm edges fail closed for routing policy.
pub(in crate::server) fn trusted_swarm_ancestry(
    members: &HashMap<String, SwarmMember>,
    session_id: &str,
) -> anyhow::Result<Vec<String>> {
    let swarm = members
        .get(session_id)
        .and_then(|m| m.swarm_id.as_deref())
        .ok_or_else(|| anyhow::anyhow!("Missing trusted swarm member '{session_id}'."))?;
    let mut ancestry = Vec::new();
    let mut seen = HashSet::new();
    let mut current = session_id;
    loop {
        if !seen.insert(current.to_string()) {
            anyhow::bail!("Cycle in trusted swarm ancestry.");
        }
        let member = members.get(current).ok_or_else(|| {
            anyhow::anyhow!("Missing trusted swarm ancestor '{current}'; routing denied.")
        })?;
        if member.swarm_id.as_deref() != Some(swarm) {
            anyhow::bail!("Swarm ancestry crosses a swarm boundary.");
        }
        ancestry.push(current.to_string());
        match member.report_back_to_session_id.as_deref() {
            Some(parent) => current = parent,
            None => break,
        }
    }
    Ok(ancestry)
}

/// Walk the `report_back_to_session_id` chain upward from `session_id`,
/// returning ancestor session ids (parent first, root last).
/// Cycles terminate this best-effort view without repeating a member.
pub(in crate::server) fn swarm_ancestors(
    members: &HashMap<String, SwarmMember>,
    session_id: &str,
) -> Vec<String> {
    let mut ancestors = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    visited.insert(session_id.to_string());
    let mut current = session_id.to_string();
    while let Some(parent) = members
        .get(&current)
        .and_then(|member| member.report_back_to_session_id.clone())
    {
        if parent == current || !visited.insert(parent.clone()) {
            break;
        }
        ancestors.push(parent.clone());
        current = parent;
    }
    ancestors
}

/// Depth of `session_id` in the spawn tree: number of ancestors reachable via
/// the report-back chain. Root coordinators (no report-back owner) are depth 0.
///
/// Test-only: the spawn tree no longer enforces a depth cap, so production code
/// does not consult depth. Kept (behind `cfg(test)`) because the spawn-tree tests
/// assert ancestor-chain depth directly.
#[cfg(test)]
pub(in crate::server) fn swarm_spawn_depth(
    members: &HashMap<String, SwarmMember>,
    session_id: &str,
) -> u32 {
    swarm_ancestors(members, session_id).len() as u32
}

/// True when `ancestor` is `session_id` itself or any transitive spawner of it.
/// Used to decide whether a requester may manage (stop/control) a target: an
/// agent owns its entire spawned subtree.
pub(in crate::server) fn swarm_is_self_or_ancestor(
    members: &HashMap<String, SwarmMember>,
    ancestor: &str,
    session_id: &str,
) -> bool {
    ancestor == session_id
        || swarm_ancestors(members, session_id)
            .iter()
            .any(|candidate| candidate == ancestor)
}
