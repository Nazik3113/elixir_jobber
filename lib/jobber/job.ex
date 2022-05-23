defmodule Jobber.Job do
    use GenServer, restart: :transient
    require Logger

    @max_retries 3

    defstruct [:work, :id, :max_retries, :parent_pid, retries: 0, status: "new"]

    defp via(key, value) do
        {:via, Registry, {Jobber.JobRegistry, key, value}}
    end

    def start_link(args) do
        args = 
            if Keyword.has_key?(args, :id) do
                args
            else
                Keyword.put(args, :id, random_job_id())
            end

        id = Keyword.get(args, :id)    
        type = Keyword.get(args, :type)

        GenServer.start_link(__MODULE__, args, name: via(id, type))
    end
    
    def init(args) do
        work = Keyword.fetch!(args, :work)
        id = Keyword.get(args, :id)
        max_retries = Keyword.get(args, :max_retries, @max_retries)
        parent_pid = Keyword.get(args, :parent_pid)

        state = %__MODULE__{id: id, work: work, max_retries: max_retries, parent_pid: parent_pid}
        {:ok, state, {:continue, :run}}
    end

    def handle_continue(:run, state) do
        try do
            new_state = state.work.() |> handle_job_result(state)

            if new_state.status === "errored" do
                Process.send_after(self(), :retry, 5000)
                {:noreply, new_state}
            else
                Logger.info("Job exiting #{state.id}")
                Supervisor.stop(state.parent_pid)
                {:stop, :normal, new_state}
            end 
        rescue
            error ->
                new_state = handle_job_result(:terminate, state, error)

                if new_state.status === "errored" do
                    Process.send_after(self(), :retry, 5000)
                    {:noreply, new_state}
                else
                    Logger.info("Job exiting with error #{inspect(error)}")
                    Supervisor.stop(state.parent_pid)
                    {:stop, :normal, new_state}
                end 
        end
    end

    defp handle_job_result({:ok, _data}, state) do
        Logger.info("Job completed #{state.id}")
        %__MODULE__{state | status: "done"}
    end

    defp handle_job_result(:error, %{status: "new"} = state) do
        Logger.info("Job errored #{state.id}")
        %__MODULE__{state | status: "errored"}
    end

    defp handle_job_result(:error, %{status: "errored"} = state) do
        Logger.info("Job retry failed #{state.id}")
        new_state = %__MODULE__{state | retries: state.retries + 1}

        if new_state.retries === state.max_retries do
            %__MODULE__{new_state | status: "failed"}
        else
            new_state
        end
    end

    defp handle_job_result(:terminate, %{status: "new"} = state, error) do
        Logger.info("Job terminated with error #{inspect(error)}")
        %__MODULE__{state | status: "errored"}
    end

    defp handle_job_result(:terminate, %{status: "errored"} = state, error) do
        Logger.info("Job retry terminated with error #{inspect(error)}")
        new_state = %__MODULE__{state | retries: state.retries + 1}

        if new_state.retries === state.max_retries do
            %__MODULE__{new_state | status: "failed"}
        else
            new_state
        end
    end

    def handle_info(:retry, state) do
        {:noreply, state, {:continue, :run}}
    end

    defp random_job_id() do
        :crypto.strong_rand_bytes(5) |> Base.url_encode64(padding: false)
    end
end