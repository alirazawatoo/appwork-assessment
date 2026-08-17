defmodule Cache.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Task.Supervisor, name: Cache.TaskSupervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Cache.Supervisor)
  end
end
