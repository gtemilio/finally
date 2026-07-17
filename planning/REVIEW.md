# Review of planning/PLAN.md

## Findings

1. High: The LLM implementation depends on a `cerebras-inference` skill that is not defined in the available project/agent skill set.

   `planning/PLAN.md:285` and `planning/PLAN.md:296` require agents to use the `cerebras-inference` skill, but that skill is not present in the available skills. Agents following the plan literally will be blocked or will invent their own LiteLLM/OpenRouter implementation. The plan should either add/install/document that skill, or replace the skill dependency with concrete implementation requirements: package, model string, provider config, request shape, structured-output validation, retries, and error handling.

2. High: The first-run promise conflicts with `OPENROUTER_API_KEY` being required.

   `planning/PLAN.md:15` says users run one Docker command and immediately get the app with the AI chat panel ready, while `planning/PLAN.md:124` says `OPENROUTER_API_KEY` is required and `planning/PLAN.md:287` says the key exists in the root `.env`. For a repo/course capstone, fresh checkouts and CI will not have that secret. The plan needs explicit behavior when the key is missing: fail startup with a clear error, disable only chat, or default chat to `LLM_MOCK=true`. Without that, backend, Docker, and E2E agents may make incompatible choices.

3. High: Database persistence is specified in two incompatible ways.

   `planning/PLAN.md:67` and `planning/PLAN.md:114` describe a project-root SQLite file at `db/finally.db`, but `planning/PLAN.md:416` uses a named Docker volume mounted at `/app/db`, and `planning/PLAN.md:419` says the project root `db/` maps to `/app/db`. A named volume is not the project root directory. The plan should choose either a bind mount (`./db:/app/db`) or a named volume (`finally-data:/app/db`) and make the path contract consistent for Dockerfile, scripts, tests, and local backend development.

4. Medium: The schema statement "All tables include a `user_id` column" is false for `users_profile`.

   `planning/PLAN.md:197` says all tables include `user_id`, but `planning/PLAN.md:199-202` defines `users_profile` with `id` and no `user_id`. That may seem small, but it affects generic repository helpers, seed logic, future multi-user assumptions, and test fixture patterns. Either exempt `users_profile` explicitly or add a separate `user_id` column and clarify primary-key semantics.

5. Medium: Trade execution lacks a precise price/source contract.

   `planning/PLAN.md:261` defines `{ticker, quantity, side}` only, and the validation guidance at `planning/PLAN.md:318-329` mentions cash/share checks but not what happens when a ticker has no current price, is not in the watchlist/cache, is invalid, is stale, or has zero/negative quantity. This is a core shared contract between manual trades, LLM trades, portfolio snapshots, and tests. The plan should specify ticker normalization, quantity bounds, price lookup rules, stale-price handling, and whether trades can introduce positions for tickers not currently watched.

6. Medium: SSE watchlist scope is ambiguous after watchlist changes.

   `planning/PLAN.md:162` says the market poller uses the union of watched tickers, while `planning/PLAN.md:178` says SSE pushes all tickers known to the system and treats that as equivalent to the watchlist in single-user mode. After adding/removing tickers, agents need to know whether removed tickers stop streaming immediately, stay in cache but stop emitting, or continue until restart. Define the source of truth for the stream and how watchlist mutations update the market-data task.

7. Low: The document appears to rely on Unicode box-drawing and arrow characters that may render incorrectly in some Windows shells.

   In this environment the title, arrows, and directory diagrams render as mojibake in PowerShell output. If agents or course materials are expected to be read from shell output, consider either enforcing UTF-8 read/write conventions or replacing decorative diagrams/arrows with ASCII. This is not an application bug, but it can slow down agent coordination.

## Overall

The plan is strong as a product and architecture brief, especially around the single-container shape, SSE choice, simulator fallback, and test strategy. The main gaps are not missing features; they are contract ambiguities that will cause different agents to implement different assumptions. I would resolve the LLM dependency/key behavior, database mount path, trade validation contract, and SSE watchlist semantics before splitting work across implementation agents.
