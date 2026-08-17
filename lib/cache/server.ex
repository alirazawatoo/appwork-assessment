defmodule Cache.Server do
  @moduledoc """
  A GenServer-backed cache that wraps a slow upstream `fetch/1`.

  V3: capped capacity, true LRU eviction, plus TTL — the entries kept are
  the `:cap` most *recently used* distinct requests, each valid only until
  its response's TTL (`Cache.Response.ttl/1`) elapses. A cache hit whose
  entry has expired is treated exactly like a miss: the stale entry is
  evicted and the request is re-fetched from upstream. A cache hit that is
  still valid "touches" its entry, moving it to the most-recently-used end
  of `order`, so eviction (once over capacity) drops the least-recently-used
  entry rather than simply the oldest-inserted one.

  `order` is a plain list (MRU-first) rather than a proper O(1) structure
  (e.g. a doubly-linked list), so touching/evicting is O(n) in the cap size.
  That's an intentional, documented tradeoff for this stage — V4 of the
  exercise (amortized O(1), out of scope here) is exactly where this would
  be replaced.

  Cache hits are answered directly from in-memory state inside
  `handle_call/3`. Cache misses are fetched asynchronously via a supervised
  `Task` so that one slow upstream call never blocks other callers —
  including other callers asking for the *same* in-flight request, whose
  calls are coalesced onto the single upstream call already in progress
  instead of triggering a second one.

  ## Options

    * `:cap` (required) - maximum number of cached entries
    * `:upstream` (required) - a 1-arity `request -> response` function
    * `:fetch_timeout` - milliseconds an upstream call may take before it
      is killed, or `:infinity` (default `5000`)
    * `:task_supervisor` - where upstream calls run (default
      `Cache.TaskSupervisor`)
    * `:name` - optional registered name

  ## Failures

  If the upstream call raises, exits, returns anything other than a
  `Cache.Response`, or exceeds `:fetch_timeout`, every caller waiting on it
  gets a `Cache.UpstreamError` raised from `fetch/2`. Nothing is cached, so
  the next fetch tries upstream again. The cache process itself never
  crashes on upstream misbehaviour.
  """

  use GenServer

  alias Cache.{Request, Response, UpstreamError}

  @behaviour Cache

  defstruct [
    :cap,
    :upstream,
    :task_supervisor,
    :fetch_timeout,
    entries: %{},
    order: [],
    size: 0,
    tasks: %{},
    pending: %{}
  ]

  @type upstream_fun :: (Request.t() -> struct())

  ## Client API

  @impl Cache
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @impl Cache
  @spec fetch(GenServer.server(), Request.t()) :: struct()
  def fetch(server, %Request{} = request) do
    # The server bounds each upstream call with :fetch_timeout, so the
    # call itself can wait as long as that takes.
    case GenServer.call(server, {:fetch, request}, :infinity) do
      {:ok, response} -> response
      {:error, reason} -> raise UpstreamError, request: request, reason: reason
    end
  end

  ## Server callbacks

  @impl GenServer
  def init(opts) do
    cap = Keyword.fetch!(opts, :cap)
    upstream = Keyword.fetch!(opts, :upstream)
    task_supervisor = Keyword.get(opts, :task_supervisor, Cache.TaskSupervisor)
    fetch_timeout = Keyword.get(opts, :fetch_timeout, 5_000)

    if not (is_integer(cap) and cap > 0) do
      raise ArgumentError, "expected :cap to be a positive integer, got: #{inspect(cap)}"
    end

    if not is_function(upstream, 1) do
      raise ArgumentError,
            "expected :upstream to be a 1-arity function, got: #{inspect(upstream)}"
    end

    if not (fetch_timeout == :infinity or (is_integer(fetch_timeout) and fetch_timeout > 0)) do
      raise ArgumentError,
            "expected :fetch_timeout to be a positive integer or :infinity, " <>
              "got: #{inspect(fetch_timeout)}"
    end

    {:ok,
     %__MODULE__{
       cap: cap,
       upstream: upstream,
       task_supervisor: task_supervisor,
       fetch_timeout: fetch_timeout
     }}
  end

  @impl GenServer
  def handle_call({:fetch, request}, from, state) do
    cond do
      fresh?(state, request) ->
        {response, _expires_at} = Map.fetch!(state.entries, request)
        {:reply, {:ok, response}, touch(state, request)}

      Map.has_key?(state.entries, request) ->
        {:noreply, state |> evict(request) |> start_fetch(request, from)}

      Map.has_key?(state.pending, request) ->
        {:noreply, add_waiter(state, request, from)}

      true ->
        {:noreply, start_fetch(state, request, from)}
    end
  end

  @impl GenServer
  def handle_info({ref, result}, %__MODULE__{tasks: tasks} = state)
      when is_map_key(tasks, ref) do
    Process.demonitor(ref, [:flush])
    {request, state} = finish_task(state, ref)

    case result do
      %Response{ttl_seconds: ttl} = response when is_integer(ttl) and ttl >= 0 ->
        {waiters, state} = pop_waiters(state, request)
        Enum.each(waiters, &GenServer.reply(&1, {:ok, response}))
        {:noreply, put_entry(state, request, response)}

      other ->
        {:noreply, fail_waiters(state, request, {:invalid_response, other})}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %__MODULE__{tasks: tasks} = state)
      when is_map_key(tasks, ref) do
    {request, state} = finish_task(state, ref)
    {:noreply, fail_waiters(state, request, exit_reason(reason))}
  end

  def handle_info({:fetch_timeout, ref}, %__MODULE__{tasks: tasks} = state)
      when is_map_key(tasks, ref) do
    %{pid: pid} = Map.fetch!(tasks, ref)
    Process.demonitor(ref, [:flush])
    Task.Supervisor.terminate_child(state.task_supervisor, pid)
    {request, state} = finish_task(state, ref)
    {:noreply, fail_waiters(state, request, :timeout)}
  end

  # Late task replies or timers for a task that already finished, plus any
  # message sent to the cache by mistake: none of them may crash it.
  def handle_info(_message, state), do: {:noreply, state}

  ## Internal helpers

  defp start_fetch(state, request, from) do
    upstream = state.upstream

    %Task{ref: ref, pid: pid} =
      Task.Supervisor.async_nolink(state.task_supervisor, fn -> upstream.(request) end)

    timer =
      if state.fetch_timeout != :infinity,
        do: Process.send_after(self(), {:fetch_timeout, ref}, state.fetch_timeout)

    task = %{request: request, pid: pid, timer: timer}

    state
    |> Map.update!(:tasks, &Map.put(&1, ref, task))
    |> Map.update!(:pending, &Map.put(&1, request, [from]))
  end

  defp finish_task(%__MODULE__{} = state, ref) do
    {%{request: request, timer: timer}, tasks} = Map.pop(state.tasks, ref)
    if timer, do: Process.cancel_timer(timer)
    {request, %__MODULE__{state | tasks: tasks}}
  end

  defp pop_waiters(%__MODULE__{} = state, request) do
    {waiters, pending} = Map.pop(state.pending, request, [])
    {waiters, %__MODULE__{state | pending: pending}}
  end

  defp fail_waiters(%__MODULE__{} = state, request, reason) do
    {waiters, state} = pop_waiters(state, request)
    Enum.each(waiters, &GenServer.reply(&1, {:error, reason}))
    state
  end

  defp exit_reason({exception, stacktrace}) when is_exception(exception) and is_list(stacktrace),
    do: {:raised, exception}

  defp exit_reason({{:nocatch, value}, stacktrace}) when is_list(stacktrace),
    do: {:exit, {:throw, value}}

  defp exit_reason(reason), do: {:exit, reason}

  defp add_waiter(state, request, from) do
    Map.update!(state, :pending, fn pending ->
      Map.update!(pending, request, &[from | &1])
    end)
  end

  # A zero TTL means "valid for this reply only": hand it out, don't store it.
  defp put_entry(state, _request, %Response{ttl_seconds: 0}), do: state

  defp put_entry(%__MODULE__{} = state, request, response) do
    now = now_ms()
    expires_at = now + :timer.seconds(Response.ttl(response))

    state = %__MODULE__{
      state
      | entries: Map.put(state.entries, request, {response, expires_at}),
        order: [request | state.order],
        size: state.size + 1
    }

    if state.size > state.cap do
      state |> drop_expired(now) |> drop_lru_over_cap()
    else
      state
    end
  end

  # Expired entries are only removed lazily on lookup, so they can still
  # hold slots. Reclaim those first rather than evicting a live entry.
  defp drop_expired(%__MODULE__{entries: entries} = state, now) do
    {expired, live} =
      Enum.split_with(state.order, fn key -> elem(Map.fetch!(entries, key), 1) <= now end)

    %__MODULE__{
      state
      | entries: Map.drop(entries, expired),
        order: live,
        size: state.size - length(expired)
    }
  end

  defp drop_lru_over_cap(%__MODULE__{size: size, cap: cap} = state) when size <= cap, do: state

  defp drop_lru_over_cap(%__MODULE__{} = state) do
    {least_recently_used, order} = List.pop_at(state.order, -1)

    %__MODULE__{
      state
      | entries: Map.delete(state.entries, least_recently_used),
        order: order,
        size: state.size - 1
    }
  end

  defp touch(%__MODULE__{} = state, request) do
    %__MODULE__{state | order: [request | List.delete(state.order, request)]}
  end

  defp evict(%__MODULE__{} = state, request) do
    %__MODULE__{
      state
      | entries: Map.delete(state.entries, request),
        order: List.delete(state.order, request),
        size: state.size - 1
    }
  end

  defp fresh?(%__MODULE__{} = state, request) do
    case Map.fetch(state.entries, request) do
      {:ok, {_response, expires_at}} -> now_ms() < expires_at
      :error -> false
    end
  end

  # Whole-second monotonic time would truncate the insert instant, so an
  # entry cached late in a second could expire up to a second early.
  defp now_ms, do: System.monotonic_time(:millisecond)
end
