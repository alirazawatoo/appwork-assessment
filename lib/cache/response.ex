defmodule Cache.Response do
  @moduledoc """
  Example response struct, used as a cache value. Each response is assumed
  unique per request. `ttl_seconds` is how long this response stays valid
  once cached (see `ttl/1`); `0` means "do not cache".
  """

  @enforce_keys [:status, :body, :ttl_seconds]
  defstruct [:status, :body, :ttl_seconds]

  @type t :: %__MODULE__{
          status: pos_integer(),
          body: term(),
          ttl_seconds: non_neg_integer()
        }

  @doc """
  The number of seconds this response stays valid once cached.
  """
  @spec ttl(t()) :: non_neg_integer()
  def ttl(%__MODULE__{ttl_seconds: ttl_seconds}), do: ttl_seconds
end
