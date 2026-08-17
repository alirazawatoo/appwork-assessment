defmodule Cache.ServerTest do
  use ExUnit.Case, async: true

  alias Cache.{Request, Response, Server, UpstreamError}

  defp request(path), do: %Request{method: :get, path: path}

  defp counting_upstream(opts \\ []) do
    delay_ms = Keyword.get(opts, :delay_ms, 0)
    ttl_seconds = Keyword.get(opts, :ttl_seconds, 60)
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    upstream = fn %Request{} = req ->
      Agent.update(counter, &(&1 + 1))
      if delay_ms > 0, do: Process.sleep(delay_ms)
      %Response{status: 200, body: req.path, ttl_seconds: ttl_seconds}
    end

    {upstream, fn -> Agent.get(counter, & &1) end}
  end

  defp wait_until_late_in_second(min_ms) do
    if Integer.mod(System.monotonic_time(:millisecond), 1000) < min_ms do
      Process.sleep(10)
      wait_until_late_in_second(min_ms)
    end
  end

  defp start_cache!(opts) do
    {upstream, call_count} = Keyword.get_lazy(opts, :upstream_pair, fn -> counting_upstream() end)
    cap = Keyword.fetch!(opts, :cap)

    server_opts = [cap: cap, upstream: upstream] ++ Keyword.take(opts, [:fetch_timeout])
    pid = start_supervised!({Server, server_opts})
    {pid, call_count}
  end

  test "fetches from upstream on a miss and returns its response" do
    {pid, call_count} = start_cache!(cap: 10)

    assert %Response{body: "/a"} = Server.fetch(pid, request("/a"))
    assert call_count.() == 1
  end

  test "serves a repeated identical request from cache without calling upstream again" do
    {pid, call_count} = start_cache!(cap: 10)

    assert Server.fetch(pid, request("/a")) == Server.fetch(pid, request("/a"))
    assert call_count.() == 1
  end

  test "evicts the oldest entry once more than cap distinct requests are inserted" do
    {pid, call_count} = start_cache!(cap: 2)

    Server.fetch(pid, request("/a"))
    Server.fetch(pid, request("/b"))
    Server.fetch(pid, request("/c"))
    assert call_count.() == 3

    # "/a" was the least recently used and should have been evicted -> refetches from upstream.
    Server.fetch(pid, request("/a"))
    assert call_count.() == 4

    # "/c" was accessed more recently than "/b" and should still be cached.
    Server.fetch(pid, request("/c"))
    assert call_count.() == 4
  end

  test "recently accessed entries survive eviction over untouched ones (true LRU)" do
    {pid, call_count} = start_cache!(cap: 2)

    Server.fetch(pid, request("/a"))
    Server.fetch(pid, request("/b"))
    assert call_count.() == 2

    # touch "/a" so it becomes the most-recently-used entry.
    Server.fetch(pid, request("/a"))
    assert call_count.() == 2

    # inserting a third distinct request evicts the least-recently-used
    # entry, which is now "/b" (untouched since insertion), not "/a".
    Server.fetch(pid, request("/c"))
    assert call_count.() == 3

    Server.fetch(pid, request("/a"))
    assert call_count.() == 3

    Server.fetch(pid, request("/b"))
    assert call_count.() == 4
  end

  test "an entry with a still-valid TTL is served from cache without recalling upstream" do
    {upstream, call_count} = counting_upstream(ttl_seconds: 60)
    {pid, _call_count} = start_cache!(cap: 10, upstream_pair: {upstream, call_count})

    Server.fetch(pid, request("/a"))
    Server.fetch(pid, request("/a"))
    assert call_count.() == 1
  end

  test "an expired entry is treated as a miss and refetched from upstream" do
    {upstream, call_count} = counting_upstream(ttl_seconds: 1)
    {pid, _call_count} = start_cache!(cap: 10, upstream_pair: {upstream, call_count})

    Server.fetch(pid, request("/a"))
    assert call_count.() == 1

    Process.sleep(1_100)

    Server.fetch(pid, request("/a"))
    assert call_count.() == 2
  end

  test "an entry cached just before a second boundary keeps its full TTL" do
    {upstream, call_count} = counting_upstream(ttl_seconds: 1)
    {pid, _call_count} = start_cache!(cap: 10, upstream_pair: {upstream, call_count})

    wait_until_late_in_second(800)
    Server.fetch(pid, request("/a"))
    Process.sleep(300)

    # 300 ms into a 1 s TTL, but already past the next whole second.
    Server.fetch(pid, request("/a"))
    assert call_count.() == 1
  end

  test "a response with a zero TTL is returned but never cached" do
    {upstream, call_count} = counting_upstream(ttl_seconds: 0)
    {pid, _call_count} = start_cache!(cap: 10, upstream_pair: {upstream, call_count})

    assert %Response{ttl_seconds: 0} = Server.fetch(pid, request("/a"))
    Server.fetch(pid, request("/a"))
    assert call_count.() == 2
  end

  test "concurrent identical-miss requests coalesce into a single upstream call" do
    {upstream, call_count} = counting_upstream(delay_ms: 50)
    {pid, _call_count} = start_cache!(cap: 10, upstream_pair: {upstream, call_count})

    results =
      1..20
      |> Task.async_stream(fn _ -> Server.fetch(pid, request("/shared")) end, timeout: :infinity)
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.uniq(results) == [%Response{status: 200, body: "/shared", ttl_seconds: 60}]
    assert call_count.() == 1
  end

  test "concurrent fetches for different requests all resolve correctly" do
    {pid, call_count} = start_cache!(cap: 50)

    results =
      1..30
      |> Task.async_stream(fn i -> Server.fetch(pid, request("/#{i}")) end, timeout: :infinity)
      |> Enum.map(fn {:ok, result} -> result end)

    assert length(Enum.uniq(results)) == 30
    assert call_count.() == 30
  end

  test "a killed coalesced caller does not disturb the other waiters" do
    {upstream, call_count} = counting_upstream(delay_ms: 100)
    {pid, _call_count} = start_cache!(cap: 10, upstream_pair: {upstream, call_count})

    doomed = spawn(fn -> Server.fetch(pid, request("/shared")) end)
    survivor = Task.async(fn -> Server.fetch(pid, request("/shared")) end)
    Process.sleep(20)
    Process.exit(doomed, :kill)

    assert %Response{body: "/shared"} = Task.await(survivor)
    assert call_count.() == 1
    assert Server.fetch(pid, request("/shared")).body == "/shared"
  end

  test "rejects an upstream that is not a 1-arity function" do
    assert {:error, {{%ArgumentError{message: message}, _stacktrace}, _child}} =
             start_supervised({Server, cap: 1, upstream: :nope})

    assert message =~ ":upstream"
  end

  test "ignores stray messages instead of crashing" do
    {pid, _call_count} = start_cache!(cap: 10)

    send(pid, :unexpected)
    send(pid, {make_ref(), :late_reply})

    assert %Response{body: "/a"} = Server.fetch(pid, request("/a"))
  end

  describe "when the upstream fails" do
    @describetag capture_log: true

    test "a raise reaches every coalesced caller, and the failure is not cached" do
      {:ok, calls} = Agent.start_link(fn -> 0 end)

      upstream = fn %Request{} ->
        Agent.update(calls, &(&1 + 1))
        Process.sleep(50)
        raise "upstream exploded"
      end

      {pid, _} = start_cache!(cap: 10, upstream_pair: {upstream, nil})

      errors =
        1..5
        |> Task.async_stream(fn _ -> catch_upstream_error(pid, request("/a")) end,
          max_concurrency: 5
        )
        |> Enum.map(fn {:ok, error} -> error end)

      assert Agent.get(calls, & &1) == 1

      for error <- errors do
        assert %UpstreamError{reason: {:raised, %RuntimeError{message: "upstream exploded"}}} =
                 error

        assert Exception.message(error) =~ "upstream exploded"
      end

      catch_upstream_error(pid, request("/a"))
      assert Agent.get(calls, & &1) == 2
    end

    test "a result that is not a valid response is an error, not a crash" do
      upstream = fn %Request{} -> {:error, :not_found} end
      {pid, _} = start_cache!(cap: 10, upstream_pair: {upstream, nil})

      assert %UpstreamError{reason: {:invalid_response, {:error, :not_found}}} =
               catch_upstream_error(pid, request("/a"))

      assert %UpstreamError{reason: {:invalid_response, _}} =
               catch_upstream_error(pid, request("/a"))
    end

    test "a slow upstream call is killed after :fetch_timeout" do
      test_pid = self()

      upstream = fn %Request{} ->
        send(test_pid, {:upstream_started, self()})
        Process.sleep(:infinity)
      end

      {pid, _} = start_cache!(cap: 10, upstream_pair: {upstream, nil}, fetch_timeout: 50)

      {elapsed_us, error} = :timer.tc(fn -> catch_upstream_error(pid, request("/a")) end)

      assert %UpstreamError{reason: :timeout} = error
      assert elapsed_us < 1_000_000
      assert_received {:upstream_started, task_pid}
      refute Process.alive?(task_pid)

      catch_upstream_error(pid, request("/a"))
      assert_received {:upstream_started, second_task_pid}
      assert second_task_pid != task_pid
    end
  end

  defp catch_upstream_error(pid, request) do
    assert_raise UpstreamError, fn -> Server.fetch(pid, request) end
  end
end
