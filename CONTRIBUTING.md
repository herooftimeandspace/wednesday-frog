# Contributing

`IMPLEMENTATION_PLAN.md` is the authoritative scope and architecture document for this repository.

Before making any change:

1. Read [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md).
2. Confirm the change still matches that plan.
3. If the change affects scope, interfaces, storage, auth, delivery behavior, deployment, plugin contracts, or HA behavior, update `IMPLEMENTATION_PLAN.md` first.

## Plugin Development

Bundled plugins live under `src/wednesday_frog/plugins/<plugin_id>/`.

Each plugin must include:

1. `manifest.json`
2. `plugin.py`

The connector class in `plugin.py` must implement the `FrogConnector` contract and provide:

- destination config schema
- destination secret schema
- channel config schema
- channel secret schema
- validation logic
- send logic
- error handling

Use the local checker before opening a PR:

```bash
wednesday-frog check
wednesday-frog check --emit-plugin-env slack
```

That command validates manifests, imports, and supported JSON Schema usage, and it can print placeholder env and Compose hints for local plugin testing.

## Repo Expectations

- Keep secrets out of source control.
- Keep runtime databases, `/data` contents, and file-backed secret files out of commits.
- Preserve the plan-first workflow so the code, UI, tests, and docs stay aligned.
