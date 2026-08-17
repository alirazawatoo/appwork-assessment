defmodule Cache.ServerTest do
  use ExUnit.Case, async: true

  alias Cache.{Request, Response, Server}

  defp request(path), do: %Request{method: :get, path: path}

  defp counting_upstream(opts \\ []) do
    delay_ms = Keyword.get(opts, :delay_ms, 0)
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    upstream = fn %Request{} = req ->
      Agent.update(counter, &(&1 + 1))
      if delay_ms > 0, do: Process.sleep(delay_ms)
      %Response{status: 200, body: req.path}
    end

    {upstream, fn -> Agent.get(counter, & &1) end}
  end

  defp start_cache!(opts) do
    {upstream, call_count} = Keyword.get_lazy(opts, :upstream_pair, fn -> counting_upstream() end)
    cap = Keyword.fetch!(opts, :cap)

    pid = start_supervised!({Server, cap: cap, upstream: upstream})
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

  test "concurrent identical-miss requests coalesce into a single upstream call" do
    {upstream, call_count} = counting_upstream(delay_ms: 50)
    {pid, _call_count} = start_cache!(cap: 10, upstream_pair: {upstream, call_count})

    results =
      1..20
      |> Task.async_stream(fn _ -> Server.fetch(pid, request("/shared")) end, timeout: :infinity)
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.uniq(results) == [%Response{status: 200, body: "/shared"}]
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
end
