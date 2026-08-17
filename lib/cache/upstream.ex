defmodule Cache.Upstream do
  @moduledoc """
  A fake "slow" internal service standing in for whatever real upstream a
  production cache would wrap. `Cache.Server` accepts any 1-arity
  `request -> response` function at `start_link/1`; this one exists so the
  cache is runnable and demonstrable on its own (e.g. from `iex -S mix`).
  Tests use their own fast, call-counting fake instead.
  """

  alias Cache.{Request, Response}

  @latency_ms 100
  @ttl_seconds 60

  @spec fetch(Request.t()) :: Response.t()
  def fetch(%Request{} = request) do
    Process.sleep(@latency_ms)

    %Response{
      status: 200,
      body: "response for request hash #{Request.hash(request)}",
      ttl_seconds: @ttl_seconds
    }
  end
end
