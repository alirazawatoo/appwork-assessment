defmodule Cache.Server do
  @moduledoc """
  A GenServer-backed cache that wraps a slow upstream `fetch/1`.

  V1: capped capacity, FIFO eviction — the cache stores responses for at
  least the last `:cap` requests and evicts the oldest entry once that cap
  is exceeded.

  Cache hits are answered directly from in-memory state inside
  `handle_call/3`. Cache misses are fetched asynchronously via a supervised
  `Task` so that one slow upstream call never blocks other callers —
  including other callers asking for the *same* in-flight request, whose
  calls are coalesced onto the single upstream call already in progress
  instead of triggering a second one.
  """

  use GenServer

  alias Cache.Request

  @behaviour Cache

  defstruct [
    :cap,
    :upstream,
    :task_supervisor,
    entries: %{},
    order: :queue.new(),
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
    GenServer.call(server, {:fetch, request}, :infinity)
  end

  ## Server callbacks

  @impl GenServer
  def init(opts) do
    cap = Keyword.fetch!(opts, :cap)
    upstream = Keyword.fetch!(opts, :upstream)
    task_supervisor = Keyword.get(opts, :task_supervisor, Cache.TaskSupervisor)

    if not (is_integer(cap) and cap > 0) do
      raise ArgumentError, "expected :cap to be a positive integer, got: #{inspect(cap)}"
    end

    {:ok, %__MODULE__{cap: cap, upstream: upstream, task_supervisor: task_supervisor}}
  end

  @impl GenServer
  def handle_call({:fetch, request}, from, state) do
    cond do
      Map.has_key?(state.entries, request) ->
        {:reply, Map.fetch!(state.entries, request), state}

      Map.has_key?(state.pending, request) ->
        {:noreply, add_waiter(state, request, from)}

      true ->
        {:noreply, start_fetch(state, request, from)}
    end
  end

  @impl GenServer
  def handle_info({ref, response}, %__MODULE__{tasks: tasks} = state)
      when is_map_key(tasks, ref) do
    Process.demonitor(ref, [:flush])
    {request, tasks} = Map.pop(tasks, ref)
    {waiters, pending} = Map.pop(state.pending, request)

    state =
      %__MODULE__{state | tasks: tasks, pending: pending}
      |> put_entry(request, response)

    Enum.each(waiters, &GenServer.reply(&1, response))
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %__MODULE__{tasks: tasks} = state)
      when is_map_key(tasks, ref) do
    {request, tasks} = Map.pop(tasks, ref)
    {waiters, pending} = Map.pop(state.pending, request)

    Enum.each(waiters, &GenServer.reply(&1, {:error, {:upstream_failed, reason}}))
    {:noreply, %__MODULE__{state | tasks: tasks, pending: pending}}
  end

  ## Internal helpers

  defp start_fetch(state, request, from) do
    %Task{ref: ref} =
      Task.Supervisor.async_nolink(state.task_supervisor, fn -> state.upstream.(request) end)

    state
    |> Map.update!(:tasks, &Map.put(&1, ref, request))
    |> Map.update!(:pending, &Map.put(&1, request, [from]))
  end

  defp add_waiter(state, request, from) do
    Map.update!(state, :pending, fn pending ->
      Map.update!(pending, request, &[from | &1])
    end)
  end

  defp put_entry(%__MODULE__{} = state, request, response) do
    entries = Map.put(state.entries, request, response)
    order = :queue.in(request, state.order)
    size = state.size + 1

    if size > state.cap do
      {{:value, oldest}, order} = :queue.out(order)
      %__MODULE__{state | entries: Map.delete(entries, oldest), order: order, size: size - 1}
    else
      %__MODULE__{state | entries: entries, order: order, size: size}
    end
  end
end
