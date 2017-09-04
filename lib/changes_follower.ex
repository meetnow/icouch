
# Created by Patrick Schneider on 25.10.2016.
# Copyright (c) 2016 MeetNow! GmbH

defmodule ChangesFollower do
  @moduledoc """
  A behaviour module for following live changes in a CouchDB.

  It is basically an extended `GenServer` and also inherits all of `GenServer`'s
  callbacks and semantics. The main difference is the expected return value of
  `init/1` and the new `handle_change/2` callback.

  ## Resilence

  `ChangesFollower` tries to be as resilent as possible, automatically
  restarting closed connections and resuming on last known sequence numbers for
  certain kinds of errors. It also checks if the server actually sends
  heartbeats within the expected timeframe and resets the connection on failures.

  ## Callbacks

  There are 7 callbacks required to be implemented in a `ChangesFollower`. By
  adding `use ChangesFollower` to your module, Elixir will automatically define
  all 7 callbacks for you, leaving it up to you to implement the ones you want
  to customize.

  ## Implementation Details

  Internally, an ibrowse worker is spawned and monitored. Therefore a
  `ChangesFollower` is not part of a load balancing pool.
  """

  require Logger

  @type on_start :: {:ok, pid} | :ignore | {:error, {:already_started, pid} | term}

  @type name :: atom | {:global, term} | {:via, module, term}

  @type gen_option :: {:debug, debug} | {:name, name} | {:timeout, timeout} |
    {:spawn_opt, Process.spawn_opt}

  @type debug :: [:trace | :log | :statistics | {:log_to_file, Path.t}]

  @type changes_follower :: pid | name | {atom, node}

  @type from :: {pid, tag :: term}

  @type option :: ICouch.open_changes_option |
    {:longpoll, boolean} | {:heartbeat, integer} | {:timeout, integer}

  @behaviour GenServer

  @callback init(args :: term) ::
    {:ok, db :: ICouch.DB.t, state} |
    {:ok, db :: ICouch.DB.t, opts :: [option], state} |
    {:ok, db :: ICouch.DB.t, opts :: [option], state, timeout | :hibernate} |
    :ignore |
    {:stop, reason :: any} when state: term

  @callback handle_change(change :: %{}, state :: term) ::
    {:ok, new_state} |
    {:ok, new_state, timeout | :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term

  @callback handle_call(request :: term, from, state :: term) ::
    {:reply, reply, new_state} |
    {:reply, reply, new_state, timeout | :hibernate} |
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term

  @callback handle_cast(request :: term, state :: term) ::
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term

  @callback handle_info(msg :: :timeout | term, state :: term) ::
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term

  @callback terminate(reason, state :: term) ::
    term when reason: :normal | :shutdown | {:shutdown, term} | term

  @callback code_change(old_vsn, state :: term, extra :: term) ::
    {:ok, new_state :: term} |
    {:error, reason :: term} when old_vsn: term | {:down, term}

  defstruct [
    :module,
    :mstate,
    :infoid,
    :db,
    :query,
    :lkg_seq, # Last known good sequence number
    :res_id,
    :tref
  ]

  defmacro __using__(_) do
    quote location: :keep do
      @behaviour ChangesFollower

      def init(args) do
        {:ok, args, nil}
      end

      def handle_change(_row, state) do
        {:ok, state}
      end

      def handle_call(_request, _from, state) do
        {:noreply, state}
      end

      def handle_cast(_request, state) do
        {:noreply, state}
      end

      def handle_info(_msg, state) do
        {:noreply, state}
      end

      def terminate(_reason, _state) do
        :ok
      end

      def code_change(_old, state, _extra) do
        {:ok, state}
      end

      defoverridable [init: 1, handle_change: 2, handle_call: 3, handle_cast: 2,
                      handle_info: 2, terminate: 2, code_change: 3]
    end
  end

  @spec start_link(module, any, options :: [gen_option]) :: on_start
  def start_link(module, args, options \\ []) when is_atom(module) and is_list(options) do
    GenServer.start_link(__MODULE__, {module, args}, options)
  end

  @spec start(module, any, options :: [gen_option]) :: on_start
  def start(module, args, options \\ []) when is_atom(module) and is_list(options) do
    GenServer.start(__MODULE__, {module, args}, options)
  end

  @spec stop(changes_follower, reason :: term, timeout) :: :ok
  def stop(changes_follower, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(changes_follower, reason, timeout)
  end

  @spec call(changes_follower, term, timeout) :: term
  def call(changes_follower, request, timeout \\ 5000) do
    GenServer.call(changes_follower, request, timeout)
  end

  @spec cast(changes_follower, term) :: :ok
  def cast(changes_follower, request) do
    GenServer.cast(changes_follower, request)
  end

  @spec reply(from, term) :: :ok
  def reply(client, reply) do
    GenServer.reply(client, reply)
  end

  @spec whereis(changes_follower) :: pid | {atom, node} | nil
  def whereis(changes_follower) do
    GenServer.whereis(changes_follower)
  end

  # -- Callbacks --

  def code_change(old_vsn, %{module: module, mstate: mstate} = state, extra) do
    case module.code_change(old_vsn, mstate, extra) do
      {:ok, new_state} -> {:ok, %{state | mstate: new_state}}
      other -> other
    end
  end

  def handle_call(request, from, %{module: module, mstate: mstate} = state) do
    case module.handle_call(request, from, mstate) do
      {:reply, reply, new_state} -> {:reply, reply, %{state | mstate: new_state}}
      {:reply, reply, new_state, timeout} -> {:reply, reply, %{state | mstate: new_state}, timeout}
      {:noreply, new_state} -> {:noreply, %{state | mstate: new_state}}
      {:noreply, new_state, timeout} -> {:noreply, %{state | mstate: new_state}, timeout}
      {:stop, reason, reply, new_state} -> {:stop, reason, reply, %{state | mstate: new_state}}
      {:stop, reason, new_state} -> {:stop, reason, %{state | mstate: new_state}}
    end
  end

  def handle_cast(request, %{module: module, mstate: mstate} = state) do
    case module.handle_cast(request, mstate) do
      {:noreply, new_state} -> {:noreply, %{state | mstate: new_state}}
      {:noreply, new_state, timeout} -> {:noreply, %{state | mstate: new_state}, timeout}
      {:stop, reason, new_state} -> {:stop, reason, %{state | mstate: new_state}}
    end
  end

  def handle_info({infoid, :start_stream}, %{infoid: infoid} = state),
    do: {:noreply, start_stream(%{state | tref: nil})}
  def handle_info({infoid, :heartbeat_missing}, %{module: module, infoid: infoid} = state) do
    Logger.warn "Heartbeat missing", via: module
    {:noreply, restart_stream(%{state | tref: nil})}
  end
  def handle_info({:ibrowse_async_headers, res_id, code, _headers}, %{module: module, res_id: res_id, query: query, lkg_seq: lkg_seq} = state) do
    case code do
      '200' ->
        case query[:since] do
          nil -> {:noreply, reset_heartbeat(state)}
          seq -> {:noreply, reset_heartbeat(%{state | lkg_seq: seq})}
        end
      '400' ->
        if lkg_seq != nil do
          Logger.warn "Bad request detected, trying last known good sequence number; failed seq was: #{inspect query[:since]}", via: module
          {:noreply, %{state | query: Map.put(query, :since, lkg_seq), lkg_seq: nil} |> stop_stream() |> start_stream()}
        else
          Logger.error "Bad request, cannot continue", via: module
          {:stop, :bad_request, state}
        end
      _ ->
        Logger.error "Request returned error code (will retry later): #{code}", via: module
        {:noreply, state |> stop_stream() |> start_stream_later()}
    end
  end
  def handle_info({:ibrowse_async_response, res_id, "\n"}, %{module: _module, res_id: res_id} = state) do
    {:noreply, reset_heartbeat(state)}
  end
  def handle_info({:ibrowse_async_response, res_id, chunk}, %{module: module, mstate: mstate, query: query, lkg_seq: lkg_seq, res_id: res_id} = state) when is_binary(chunk) do
    if query[:feed] == :continuous do
      chunk
        |> String.split("\n")
        |> Enum.filter_map(&String.length(&1) > 0, &Poison.decode!/1)
        |> case do
          [] -> :empty
          [%{"error" => error} = change | _] -> {:error, error, change["reason"]}
          [%{"seq" => last_seq} = change] -> {[change], last_seq || lkg_seq}
          changes -> {changes, get_last_seq(changes, lkg_seq)}
      end
    else
      chunk
        |> Poison.decode!()
        |> case do
          %{"error" => error} = change ->
            {:error, error, change["reason"]}
          %{"results" => changes, "last_seq" => last_seq} ->
            {changes, last_seq}
      end
    end
    |>
    case do
      :empty ->
        {:noreply, state}
      {:error, error, reason} ->
        Logger.error "Received error: #{error} - #{reason}", via: module
        {:noreply, state}
      {changes, last_seq} ->
        Logger.debug "Received changes for: #{Enum.join(Enum.map(changes, &(&1["id"])), "  ")}", via: module
        query = Map.put(query, :since, last_seq)
        case handle_changes(changes, module, mstate) do
          {:ok, new_state} -> {:noreply, %{state | mstate: new_state, query: query} |> reset_heartbeat}
          {:ok, new_state, timeout} -> {:noreply, %{state | mstate: new_state, query: query} |> reset_heartbeat, timeout}
          {:stop, reason, new_state} -> {:stop, reason, %{state | mstate: new_state, query: query} |> reset_heartbeat}
        end
    end
  end
  def handle_info({:ibrowse_async_response, res_id, {:error, reason}}, %{module: module, res_id: res_id} = state) do
    Logger.error "Error: #{inspect(reason)}", via: module
    state = cancel_timer(state)
    :timer.sleep(:rand.uniform(500))
    {:noreply, start_stream(state)}
  end
  def handle_info({:ibrowse_async_response_timeout, res_id}, %{module: module, res_id: res_id} = state) do
    Logger.debug "Request timed out", via: module
    {:noreply, restart_stream(state)}
  end
  def handle_info({:ibrowse_async_response_end, res_id}, %{module: module, res_id: res_id} = state) do
    Logger.debug "Response ended", via: module
    state = cancel_timer(state)
    :timer.sleep(:rand.uniform(500))
    {:noreply, start_stream(state)}
  end
  def handle_info({:DOWN, _, :process, ibworker, reason}, %{module: module, db: %{server: %{direct: ibworker} = server} = db} = state) do
    Logger.error "Connection process died: #{inspect reason}", via: module
    state = cancel_timer(%{state | db: %{db | server: %{server | direct: nil}}})
    :timer.sleep(:rand.uniform(500))
    {:noreply, start_stream(state)}
  end
  def handle_info(msg, %{module: module, mstate: mstate} = state) do
    Logger.debug "Other message: #{inspect(msg)}", via: module
    case module.handle_info(msg, mstate) do
      {:noreply, new_state} -> {:noreply, %{state | mstate: new_state}}
      {:noreply, new_state, timeout} -> {:noreply, %{state | mstate: new_state}, timeout}
      {:stop, reason, new_state} -> {:stop, reason, %{state | mstate: new_state}}
    end
  end

  def init({module, args}) do
    Process.flag :trap_exit, true
    case module.init(args) do
      {:ok, %ICouch.DB{} = db, mstate} -> {:ok, %__MODULE__{module: module, mstate: mstate, infoid: make_ref(), db: db} |> parse_options([]) |> start_stream()}
      {:ok, %ICouch.DB{} = db, opts, mstate} -> {:ok, %__MODULE__{module: module, mstate: mstate, infoid: make_ref(), db: db} |> parse_options(opts) |> start_stream()}
      {:ok, %ICouch.DB{} = db, opts, mstate, timeout} -> {:ok, %__MODULE__{module: module, mstate: mstate, infoid: make_ref(), db: db} |> parse_options(opts) |> start_stream(), timeout}
      :ignore -> :ignore
      {:stop, reason} -> {:stop, reason}
    end
  end

  def terminate(reason, %{module: module, mstate: mstate, db: %{server: %{direct: ibworker}}}) do
    if ibworker != nil do
      :ibrowse.stop_worker_process(ibworker)
    end
    module.terminate(reason, mstate)
  end

  # -- Private --

  defp parse_options(%{db: %{server: server} = db} = state, opts) do
    query = Map.merge(%{heartbeat: 60_000, timeout: 7_200_000}, Map.new(opts))
      |> Map.put(:feed, :continuous)
      |> Map.pop(:longpoll)
      |> case do
        {true, query} -> %{query | feed: :longpoll}
        {_, query} -> query
      end
    r_timeout = case query[:timeout] do
      nil -> nil
      t -> t + 5_000
    end
    %{state | db: %{db | server: %{server | timeout: r_timeout}}, query: query}
  end

  defp start_stream(%{db: %{server: %{direct: nil, uri: uri} = server} = db} = state) do
    {:ok, ibworker} = URI.to_string(uri) |> to_charlist() |> :ibrowse.spawn_worker_process()
    Process.monitor(ibworker)
    start_stream(%{state | db: %{db | server: %{server | direct: ibworker}}})
  end
  defp start_stream(%{module: module, db: db, query: query, res_id: old_res_id} = state) do
    ib_options = [stream_to: self(), stream_chunk_size: :infinity] ++ (if query[:feed] == :continuous, do: [stream_full_chunks: true], else: [])
    case ICouch.DB.send_raw_req(db, {"_changes", query}, :get, nil, [{"Accept", "application/json"}], ib_options) do
      {:ibrowse_req_id, res_id} ->
        if old_res_id == nil do
          Logger.info "Started stream", via: module
        else
          Logger.info "Restarted stream", via: module
        end
        %{state | res_id: res_id}
          |> reset_heartbeat()
      {:error, {:conn_failed, _}} ->
        Logger.warn "Connection failed, will retry later", via: module
        state |> stop_stream() |> start_stream_later()
      {:error, :sel_conn_closed} ->
        restart_stream(state)
    end
  end

  defp start_stream_later(%{infoid: infoid} = state) do
    {:ok, tref} = :timer.send_after(60_000 + :rand.uniform(500), {infoid, :start_stream})
    %{state | tref: tref}
  end

  defp stop_stream(%{db: %{server: %{direct: nil}}} = state),
    do: cancel_timer(state)
  defp stop_stream(%{db: %{server: %{direct: ibworker} = server} = db, res_id: res_id} = state) do
    state = cancel_timer(state)
    :ibrowse.stop_worker_process(ibworker)
    if res_id != nil do
      receive do
        {:ibrowse_async_response, ^res_id, {:error, :closing_on_request}} -> nil
      after
        10 -> nil
      end
    end
    receive do
      {:DOWN, _, :process, ^ibworker, _} -> nil
    after
      10 -> nil
    end
    %{state | db: %{db | server: %{server | direct: nil}}}
  end

  defp restart_stream(state) do
    state = stop_stream(state)
    :timer.sleep(:rand.uniform(500))
    start_stream(state)
  end

  defp cancel_timer(%{tref: nil} = state),
    do: state
  defp cancel_timer(%{tref: tref} = state) do
    :timer.cancel(tref)
    %{state | tref: nil}
  end

  defp reset_heartbeat(%{query: query, infoid: infoid} = state) do
    state = cancel_timer(state)
    case query do
      %{feed: :continuous, heartbeat: heartbeat} ->
        {:ok, tref} = :timer.send_after(heartbeat * 2, {infoid, :heartbeat_missing})
        %{state | tref: tref}
      _ ->
        state
    end
  end

  defp get_last_seq(changes, last_seq),
    do: Enum.reduce(changes, last_seq, fn (change, acc) -> change["seq"] || acc end)

  defp handle_changes([], _, mstate),
    do: {:ok, mstate}
  defp handle_changes([last_change], module, mstate),
    do: module.handle_change(last_change, mstate)
  defp handle_changes([change | tchanges], module, mstate) do
    case module.handle_change(change, mstate) do
      {:ok, new_state} -> handle_changes(tchanges, module, new_state)
      {:ok, new_state, _} -> handle_changes(tchanges, module, new_state)
      other -> other
    end
  end
end
