defmodule Cache.Response do
  @moduledoc """
  Example response struct, used as a cache value. Each response is assumed
  unique per request.
  """

  @enforce_keys [:status, :body]
  defstruct [:status, :body]

  @type t :: %__MODULE__{
          status: pos_integer(),
          body: term()
        }
end
