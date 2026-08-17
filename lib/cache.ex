defmodule Cache do
  @moduledoc """
  Behaviour for a cache that wraps a slow upstream `fetch/1` with an
  identical interface, keyed by request struct and valued by response
  struct.

  V0 of the exercise: a minimal contract, implemented by `Cache.Server`.
  """

  @type request :: struct()
  @type response :: struct()

  @callback start_link(keyword()) :: GenServer.on_start()
  @callback fetch(GenServer.server(), request()) :: response()
end
