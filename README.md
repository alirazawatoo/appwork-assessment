# Cache

An Elixir/OTP implementation of the AppWork LRU + TTL cache exercise:
a `GenServer`-backed cache that wraps a slow upstream `fetch(req) -> res`, keyed by request
struct and valued by response struct, supporting capped capacity, true LRU eviction, TTL expiry,
and high-concurrency access (cache misses run on a supervised `Task` and coalesce concurrent
callers asking for the same in-flight request onto a single upstream call).

Currently at **V3** (LRU + TTL) of the exercise's progressive V0–V4 scope. See
[`ARCHITECTURE.md`](ARCHITECTURE.md) for the class/module diagram, data flow, and high-level
design, and the git history/tags (`v1`, `v2`, `v3`) for how each version was built on top of
the last.

## Running

```
mix deps.get
mix test
```

Try it interactively:

```
iex -S mix
```

```elixir
{:ok, pid} = Cache.Server.start_link(cap: 100, upstream: &Cache.Upstream.fetch/1)
req = %Cache.Request{method: :get, path: "/users/1"}
Cache.Server.fetch(pid, req) # first call: ~100ms, hits the fake upstream
Cache.Server.fetch(pid, req) # second call: instant, served from cache
```

## Project structure

```
lib/
  cache.ex               # Cache behaviour (V0): the fetch/2 + start_link/1 contract
  cache/
    application.ex       # OTP application; starts Cache.TaskSupervisor
    request.ex            # Cache.Request struct + hash/1 (:erlang.phash2/1)
    response.ex           # Cache.Response struct + ttl/1 (V3)
    server.ex             # Cache.Server: the GenServer-backed cache implementation
    upstream.ex           # Cache.Upstream: fake slow upstream for manual/iex use

test/
  test_helper.exs
  cache/
    server_test.exs       # eviction, TTL, and concurrency/coalescing tests
```

## Requirements coverage

| Version | Adds | Status |
| --- | --- | --- |
| V0 | Cache interface (`Cache` behaviour) | done |
| V1 | Capped-capacity cache, FIFO eviction | done |
| V2 | True LRU (recency-based eviction) | done |
| V3 | LRU + TTL | done |
| V4 | O(1) fetch | not attempted |

## Test coverage

`test/cache/server_test.exs` (8 tests) verifies:

- A miss calls upstream and returns its response; a repeated identical request is served from
  cache without a second upstream call.
- Eviction drops the least-recently-used entry once more than `:cap` distinct requests are
  seen — and that touching (re-fetching) an entry protects it from eviction ahead of untouched
  ones (the behavior that distinguishes true LRU from plain FIFO).
- An entry within its TTL is served from cache; an entry past its TTL is treated as a miss and
  refetched.
- Concurrent identical-miss requests coalesce onto a single upstream call (no cache stampede).
- Concurrent fetches for different requests all resolve correctly and independently.

## Git history

Each version is its own commit, in order:

| Commit | What it adds |
| --- | --- |
| `Scaffold Elixir/OTP mix project` | `mix new . --app cache --sup` — base project structure |
| `Implement V1: capped cache with async, coalesced upstream fetches` | Cache behaviour, example structs, `Cache.Server` with cap-bounded FIFO eviction; misses run on a supervised `Task` so they never block other callers, and concurrent identical misses coalesce onto one upstream call |
| `Implement V2: true LRU eviction (recency-based)` | Cache hits now "touch" their entry; eviction drops the least-recently-used entry instead of the oldest-inserted one |
| `Implement V3: add TTL expiry to the LRU cache` | Responses carry a TTL; an expired hit is treated as a miss and refetched |
| `Add ARCHITECTURE.md: class diagram, data flow, design notes` | Class/module diagram, hit and miss/coalescing data-flow diagrams, high-level design write-up |

Tags `v1`, `v2`, `v3` point at each version's commit — `git checkout v2` gets you exactly that
stage in isolation.
