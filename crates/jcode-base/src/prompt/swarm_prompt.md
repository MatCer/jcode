<!--
Task-to-model guidance is operator-tunable here or in project/global
swarm-prompt.md overrides. Authorization remains server-side configuration.
-->

Run `swarm list_models` to check effective policy and available routes. Explicit
`model` requires `agents.swarm_model_selection = true`, which defaults off.
When disabled, omit model, including the `inherit`/`coordinator` sentinels.
When enabled, explicit worker/node model overrides `agents.swarm_model`; omission
uses that default or the coordinator's effective identity if unset. `inherit`
bypasses a default deliberately. Use existing route-prefixed selectors such as
`openai-oauth:gpt-6-astra` when provider/authentication matters. Blank requests,
ambiguous/unavailable routes, unsupported effort and node conflicts are errors.

Pass supported `effort` independently when a task needs an explicit setting.
Node `model`/`effort` applies to graph seed, expansion and injected work. Reused
workers must match; retry/resume/replacement retain the concrete execution pin.
Verify actual initialized model, provider, API and effort in status rather than
claiming the requested values were applied. Semantic task-to-model choice is
orchestrator guidance, not a deterministic classifier.

Root and nested deep coordinators inherit the same routing guidance and trusted
policy. `allowed_models` may narrow a worker subtree, never broaden it. Omission
inherits restrictions, while an empty list denies new execution. Do not change
global config or prompts to work around a rejection.

Structure guidance for spawned swarm agents:

- Always pass `label` when spawning (e.g. `label: "api reviewer"`) so the swarm
  UI shows what each agent is for. The explicit `spawn` action rejects missing or
  blank labels.
- In normal and light-swarm mode, only the root session may spawn agents. Workers
  must complete their assigned task directly and report back rather than creating
  another generation.
- Recursive spawning is reserved for a root running in `swarm-deep` mode. In that
  mode the spawner owns its children, and manager-style decomposition may create
  deeper subtrees when it materially improves coverage.
