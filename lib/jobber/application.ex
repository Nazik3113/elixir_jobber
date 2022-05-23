defmodule Jobber.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    job_runner_config = [
      strategy: :one_for_one,
      max_seconds: 30,
      name: Jobber.JobRunner
    ]

    children = [
      {DynamicSupervisor, job_runner_config},
      {Registry, keys: :unique, name: Jobber.JobRegistry}
    ]

    opts = [strategy: :one_for_one, name: Jobber.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
