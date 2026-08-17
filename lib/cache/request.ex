defmodule Cache.Request do
  @moduledoc """
  Example request struct, used directly as a cache key (Elixir maps hash
  arbitrary terms internally in O(1) average), plus an explicit hashing
  helper per the exercise's assumption that one exists.
  """

  @enforce_keys [:method, :path]
  defstruct [:method, :path, params: %{}]

  @type t :: %__MODULE__{
          method: atom(),
          path: String.t(),
          params: map()
        }

  @doc """
  A nearly-unique integer hash for a request.
  """
  @spec hash(t()) :: integer()
  def hash(%__MODULE__{} = request), do: :erlang.phash2(request)
end
