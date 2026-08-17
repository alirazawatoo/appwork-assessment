defmodule Cache.UpstreamError do
  @moduledoc """
  Raised by `Cache.Server.fetch/2` in every caller waiting on an upstream
  call that failed. Failures are never cached: the next fetch of the same
  request calls upstream again.

  `reason` is one of:

    * `{:raised, exception}` - the upstream function raised
    * `{:exit, reason}` - the upstream task exited or threw
    * `{:invalid_response, term}` - upstream returned something other than
      a `Cache.Response` with a non-negative integer TTL
    * `:timeout` - upstream did not answer within the server's `:fetch_timeout`
  """

  defexception [:request, :reason]

  @impl true
  def message(%__MODULE__{request: request, reason: reason}) do
    "upstream fetch failed for #{inspect(request)}: " <> describe(reason)
  end

  defp describe({:raised, exception}), do: Exception.message(exception)
  defp describe({:exit, reason}), do: "exited with #{inspect(reason)}"
  defp describe({:invalid_response, term}), do: "invalid response #{inspect(term)}"
  defp describe(:timeout), do: "timed out"
end
