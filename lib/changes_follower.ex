
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

  ## Additional Note

  When setting the `doc_ids` option, any given `filter` option will be ignored.
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

  @callback handle_error(error :: term, state :: term) ::
    {:retry, new_state} |
    {:retry, wait_time :: integer | :infinity, new_state} |
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
    :tref,
    :buffer
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

      def handle_error(_error, state) do
        {:retry, 60_000, state}
      end

      def terminate(_reason, _state) do
        :ok
      end

      def code_change(_old, state, _extra) do
        {:ok, state}
      end

      defoverridable [init: 1, handle_change: 2, handle_call: 3, handle_cast: 2,
                      handle_info: 2, handle_error: 2, terminate: 2, code_change: 3]
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

  @spec retry_now(changes_follower) :: :ok
  def retry_now(changes_follower) do
    send(changes_follower, :'$changes_follower_retry_now')
    :ok
  end

  # -- Callbacks --

  def code_change(old_vsn, %__MODULE__{module: module, mstate: mstate} = state, extra) do
    case module.code_change(old_vsn, mstate, extra) do
      {:ok, new_state} -> {:ok, %{state | mstate: new_state}}
      other -> other
    end
  end

  def handle_call(request, from, %__MODULE__{module: module, mstate: mstate} = state) do
    case module.handle_call(request, from, mstate) do
      {:reply, reply, new_state} -> {:reply, reply, %{state | mstate: new_state}}
      {:reply, reply, new_state, timeout} -> {:reply, reply, %{state | mstate: new_state}, timeout}
      {:noreply, new_state} -> {:noreply, %{state | mstate: new_state}}
      {:noreply, new_state, timeout} -> {:noreply, %{state | mstate: new_state}, timeout}
      {:stop, reason, reply, new_state} -> {:stop, reason, reply, %{state | mstate: new_state}}
      {:stop, reason, new_state} -> {:stop, reason, %{state | mstate: new_state}}
    end
  end

  def handle_cast(request, %__MODULE__{module: module, mstate: mstate} = state) do
    case module.handle_cast(request, mstate) do
      {:noreply, new_state} -> {:noreply, %{state | mstate: new_state}}
      {:noreply, new_state, timeout} -> {:noreply, %{state | mstate: new_state}, timeout}
      {:stop, reason, new_state} -> {:stop, reason, %{state | mstate: new_state}}
    end
  end

  def handle_info(:'$changes_follower_retry_now', %__MODULE__{db: %{server: %{direct: nil}}} = state) do
    state |> cancel_timer() |> start_stream()
  end
  def handle_info(:'$changes_follower_retry_now', state) do
    {:noreply, state}
  end
  def handle_info({infoid, :start_stream}, %__MODULE__{infoid: infoid} = state),
    do: start_stream(%{state | tref: nil})
  def handle_info({infoid, :heartbeat_missing}, %__MODULE__{module: module, infoid: infoid} = state) do
    Logger.warn "Heartbeat missing", via: module
    restart_stream(%{state | tref: nil})
  end
  def handle_info({:ibrowse_async_headers, res_id, code, _headers}, %__MODULE__{module: module, res_id: res_id, query: query, lkg_seq: lkg_seq} = state) do
    case code do
      '200' ->
        case query[:since] do
          nil -> {:noreply, reset_heartbeat(state)}
          seq -> {:noreply, reset_heartbeat(%{state | lkg_seq: seq})}
        end
      '400' ->
        if lkg_seq != nil do
          Logger.warn "Bad request detected, trying last known good sequence number; failed seq was: #{inspect query[:since]}", via: module
          %{state | query: Map.put(query, :since, lkg_seq), lkg_seq: nil} |> stop_stream() |> start_stream()
        else
          Logger.error "Bad request, cannot continue", via: module
          {:stop, :bad_request, state}
        end
      _ ->
        Logger.error "Request returned error code: #{code}", via: module
        reason = case ICouch.RequestError.parse_status_code(code) do
          {:error, reason} -> reason
          :ok -> {:status_code, List.to_integer(code)}
        end
        handle_error(reason, state |> stop_stream())
    end
  end
  def handle_info({:ibrowse_async_response, res_id, "\n"}, %__MODULE__{module: _module, res_id: res_id} = state) do
    {:noreply, reset_heartbeat(state)}
  end
  def handle_info({:ibrowse_async_response, res_id, chunk}, %__MODULE__{module: module, mstate: mstate, query: query, lkg_seq: lkg_seq, res_id: res_id, buffer: buffer} = state) when is_binary(chunk) do
    if query[:feed] == :continuous do
      [buffer | blocks] = buffer <> chunk
        |> String.split("\n") # always yields a list of at least one entry
        |> Enum.reverse()

      blocks
        |> Enum.filter(&String.length(&1) > 0)
        |> Enum.reverse()
        |> Enum.map(&Poison.decode!/1)
        |> case do
          [] -> {:empty, buffer}
          [%{"error" => error} = change | _] -> {:error, error, change["reason"]}
          [%{"id" => _, "seq" => last_seq} = change] -> {:ok, [change], last_seq || lkg_seq, buffer}
          [%{"last_seq" => last_seq}] -> {:ok, [], last_seq || lkg_seq, buffer}
          changes -> {:ok, Enum.filter(changes, fn %{"id" => _} -> true; _ -> false end), get_last_seq(changes, lkg_seq), buffer}
      end
    else
      chunk
        |> Poison.decode!()
        |> case do
          %{"error" => error} = change ->
            {:error, error, change["reason"]}
          %{"results" => changes, "last_seq" => last_seq} ->
            {:ok, changes, last_seq, ""}
      end
    end
    |>
    case do
      {:empty, buffer} ->
        {:noreply, %{state | buffer: buffer}}
      {:error, error, reason} ->
        Logger.error "Received error: #{error} - #{reason}", via: module
        {:noreply, %{state | buffer: ""}}
      {:ok, changes, last_seq, buffer} ->
        changes = if query[:include_docs] do
          Enum.map(changes, fn
            %{"doc" => doc} = row ->
              %{row | "doc" => ICouch.Document.from_api!(doc)}
            other ->
              other
          end)
        else
          changes
        end
        Logger.debug "Received changes for: #{inspect Enum.map(changes, &(&1["id"]))}", via: module
        query = Map.put(query, :since, last_seq)
        case handle_changes(changes, module, mstate) do
          {:ok, new_state} -> {:noreply, %{state | mstate: new_state, query: query, buffer: buffer} |> reset_heartbeat}
          {:ok, new_state, timeout} -> {:noreply, %{state | mstate: new_state, query: query, buffer: buffer} |> reset_heartbeat, timeout}
          {:stop, reason, new_state} -> {:stop, reason, %{state | mstate: new_state, query: query, buffer: buffer} |> reset_heartbeat}
        end
    end
  end
  def handle_info({:ibrowse_async_response, res_id, {:error, reason}}, %__MODULE__{module: module, res_id: res_id} = state) do
    Logger.error "Error: #{inspect(reason)}", via: module
    handle_error(reason, state |> cancel_timer())
  end
  def handle_info({:ibrowse_async_response_timeout, res_id}, %__MODULE__{module: module, res_id: res_id} = state) do
    Logger.debug "Request timed out (usually as expected), will restart", via: module
    restart_stream(state)
  end
  def handle_info({:ibrowse_async_response_end, res_id}, %__MODULE__{module: module, res_id: res_id} = state) do
    Logger.debug "Response ended (usually as expected), will restart", via: module
    restart_stream(state |> cancel_timer())
  end
  def handle_info({:DOWN, _, :process, ibworker, reason}, %__MODULE__{module: module, db: %{server: %{direct: ibworker} = server} = db} = state) do
    Logger.error "Connection process died, will restart: #{inspect reason}", via: module
    state = cancel_timer(%{state | db: %{db | server: %{server | direct: nil}}})
    :timer.sleep(:rand.uniform(500))
    start_stream(state)
  end
  def handle_info(msg, %__MODULE__{module: module, mstate: mstate} = state) do
    case module.handle_info(msg, mstate) do
      {:noreply, new_state} -> {:noreply, %{state | mstate: new_state}}
      {:noreply, new_state, timeout} -> {:noreply, %{state | mstate: new_state}, timeout}
      {:stop, reason, new_state} -> {:stop, reason, %{state | mstate: new_state}}
    end
  end

  def init({module, args}) do
    Process.flag :trap_exit, true
    case module.init(args) do
      {:ok, %ICouch.DB{} = db, mstate} ->
        %__MODULE__{module: module, mstate: mstate, infoid: make_ref(), db: db, buffer: ""}
          |> parse_options([])
          |> start_stream()
          |> case do
            {:noreply, state} -> {:ok, state}
            {:stop, reason, _} -> {:stop, reason}
            other -> other
          end
      {:ok, %ICouch.DB{} = db, opts, mstate} ->
        %__MODULE__{module: module, mstate: mstate, infoid: make_ref(), db: db, buffer: ""}
          |> parse_options(opts)
          |> start_stream()
          |> case do
            {:noreply, state} -> {:ok, state}
            {:stop, reason, _} -> {:stop, reason}
            other -> other
          end
      {:ok, %ICouch.DB{} = db, opts, mstate, timeout} ->
        %__MODULE__{module: module, mstate: mstate, infoid: make_ref(), db: db, buffer: ""}
          |> parse_options(opts)
          |> start_stream()
          |> case do
            {:noreply, state} -> {:ok, state, timeout}
            {:stop, reason, _} -> {:stop, reason}
            other -> other
          end
      :ignore ->
        :ignore
      {:stop, reason} ->
        {:stop, reason}
    end
  end

  def terminate(reason, %__MODULE__{module: module, mstate: mstate, db: %{server: %{direct: ibworker}}}) do
    if ibworker != nil do
      :ibrowse.stop_worker_process(ibworker)
    end
    module.terminate(reason, mstate)
  end

  # -- Private --

  defp parse_options(%__MODULE__{db: %{server: server} = db} = state, opts) do
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

  defp start_stream(%__MODULE__{db: %{server: %{direct: nil, uri: uri} = server} = db} = state) do
    {:ok, ibworker} = URI.to_string(uri) |> to_charlist() |> :ibrowse.spawn_worker_process()
    Process.monitor(ibworker)
    start_stream(%{state | db: %{db | server: %{server | direct: ibworker}}})
  end
  defp start_stream(%__MODULE__{module: module, db: db, query: query, res_id: old_res_id} = state) do
    ib_options = [stream_to: self(), stream_chunk_size: :infinity] ++ (if query[:feed] == :continuous, do: [stream_full_chunks: true], else: [])
    {query, method, body, headers} = case query do
      %{doc_ids: doc_ids} ->
        {Map.delete(query, :doc_ids) |> Map.put(:filter, "_doc_ids"), :post, Poison.encode!(%{"doc_ids" => doc_ids}), [{"Content-Type", "application/json"}, {"Accept", "application/json"}]}
      _ ->
        {query, :get, nil, [{"Accept", "application/json"}]}
    end
    case ICouch.DB.send_raw_req(db, {"_changes", query}, method, body, headers, ib_options) do
      {:ibrowse_req_id, res_id} ->
        if old_res_id == nil do
          Logger.info "Started stream", via: module
        else
          Logger.info "Restarted stream", via: module
        end
        {:noreply, %{state | res_id: res_id} |> reset_heartbeat()}
      {:error, {:conn_failed, _} = reason} ->
        Logger.warn "Connection failed", via: module
        handle_error(reason, state |> stop_stream())
      {:error, :sel_conn_closed} ->
        restart_stream(state)
    end
  end

  defp start_stream_later(%__MODULE__{infoid: infoid} = state, wait_time) do
    {:ok, tref} = :timer.send_after(wait_time + :rand.uniform(500), {infoid, :start_stream})
    %{state | tref: tref}
  end

  defp stop_stream(%__MODULE__{db: %{server: %{direct: nil}}} = state),
    do: cancel_timer(state)
  defp stop_stream(%__MODULE__{db: %{server: %{direct: ibworker} = server} = db, res_id: res_id} = state) do
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

  defp cancel_timer(%__MODULE__{tref: nil} = state),
    do: state
  defp cancel_timer(%__MODULE__{tref: tref} = state) do
    :timer.cancel(tref)
    %{state | tref: nil}
  end

  defp reset_heartbeat(%__MODULE__{query: query, infoid: infoid} = state) do
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
    do: Enum.reduce(changes, last_seq, fn change, acc -> change["seq"] || change["last_seq"] || acc end)

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

  defp handle_error(reason, %__MODULE__{module: module, mstate: mstate} = state) do
    case module.handle_error(reason, mstate) do
      {:retry, new_state} ->
        :timer.sleep(:rand.uniform(500))
        %{state | mstate: new_state}
          |> start_stream()
      {:retry, :infinity, new_state} ->
        {:noreply, %{state | mstate: new_state}}
      {:retry, wait_time, new_state} when is_integer(wait_time) ->
        {:noreply, %{state | mstate: new_state} |> start_stream_later(wait_time)}
      {:stop, reason, new_state} ->
        {:stop, reason, %{state | mstate: new_state}}
    end
  end
end
