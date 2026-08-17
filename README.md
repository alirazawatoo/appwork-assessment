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
