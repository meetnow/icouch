
# Created by Patrick Schneider on 01.09.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule ChangesFollowerTest do
  use ExUnit.Case

  defmodule MyChangesFollower do
    use ChangesFollower

    def init({:normal, d}) do
      {:ok, d, [timeout: 10_800_000], %{mode: :normal, received: []}}
    end
    def init({:include_docs, d}) do
      {:ok, d, [timeout: 10_800_000, include_docs: true, doc_ids: ["changed_document"]], %{mode: :include_docs, received: []}}
    end
    def init({:resilent, d}) do
      {:ok, d, [since: 3, heartbeat: 150], %{mode: :resilent, received: []}}
    end

    def handle_change(%{"id" => "stop_document"}, state) do
      {:stop, :change_stop, state}
    end
    def handle_change(row, %{mode: :include_docs, received: received} = state) do
      cond do
        row == %{
            "changes" => [%{"rev" => "1-a4ed57fb1018f3d423d0adec26f14dfc"}],
            "doc" => %ICouch.Document{id: "changed_document",
              rev: "1-a4ed57fb1018f3d423d0adec26f14dfc", fields: %{
                "_id" => "changed_document",
                "_rev" => "1-a4ed57fb1018f3d423d0adec26f14dfc",
                "old" => "one"
              },
              attachment_data: %{}, attachment_order: []},
            "id" => "changed_document", "seq" => 4} ->
          {:ok, %{state | received: received ++ [:changed_document]} |> check_check()}
      end
    end
    def handle_change(row, %{received: received} = state) do
      state = cond do
        row == %{"changes" => [%{"rev" => "1-946f699664e83aa36fc2833fa634adca"}], "id" => "new_document", "seq" => 1} ->
          %{state | received: received ++ [:new_document]}
        row == %{"changes" => [%{"rev" => "2-eec205a9d413992850a6e32678485900"}], "deleted" => true, "id" => "deleted_document", "seq" => 3} ->
          %{state | received: received ++ [:deleted_document]}
        row == %{"changes" => [%{"rev" => "1-a4ed57fb1018f3d423d0adec26f14dfc"}], "id" => "changed_document", "seq" => 4} ->
          %{state | received: received ++ [:changed_document]}
        row == %{"changes" => [], "id" => "changed_document", "seq" => 5} ->
          state
      end
      {:ok, state |> check_check()}
    end

    def handle_error(_, state) do
      {:retry, state}
    end

    def handle_call(:check, from, state) do
      {:noreply, Map.put(state, :check_cb, from) |> check_check()}
    end
    def handle_call({:reply, :timeout}, _from, state) do
      {:reply, :ok, state, 1_000}
    end
    def handle_call({:noreply, :timeout}, from, state) do
      ChangesFollower.reply(from, :ok)
      {:noreply, state, 1_000}
    end
    def handle_call(:call_stop, _from, state) do
      {:stop, :call_stop, :ok, state}
    end

    def handle_cast({:cast_test, from}, state) do
      send(from, :cast_test)
      {:noreply, state}
    end
    def handle_cast(:cast_stop, state) do
      {:stop, :cast_stop, state}
    end

    def handle_info({:other_message_test, from}, state) do
      send(from, :other_message_test)
      {:noreply, state}
    end

    defp check_check(%{mode: :normal, check_cb: cb, received: [:new_document, :deleted_document, :changed_document]} = state) do
      ChangesFollower.reply(cb, :ok)
      Map.delete(state, :check_cb)
    end
    defp check_check(%{mode: mode, check_cb: cb, received: [:changed_document]} = state) when mode in [:include_docs, :resilent] do
      ChangesFollower.reply(cb, :ok)
      Map.delete(state, :check_cb)
    end
    defp check_check(state) do
      state
    end
  end

  test "normal operation" do
    s = ICouch.server_connection("http://192.168.99.100:8000/")
    d = ICouch.DB.new(s, "changes_test_db")

    :meck.expect(:ibrowse, :spawn_worker_process, &start_ibworker_proc/1)
    :meck.expect(:ibrowse, :stop_worker_process, &stop_ibworker_proc/1)
    :meck.expect(:ibrowse, :send_req_direct, fn (ibworker,
        'http://192.168.99.100:8000/changes_test_db/_changes?feed=continuous&heartbeat=60000&timeout=10800000',
        [{"Accept", "application/json"}], :get, [], ib_options, 10_805_000) ->
      if length(ib_options) === 4 and
          ib_options[:response_format] === :binary and
          ib_options[:stream_to] === self() and
          ib_options[:stream_chunk_size] === :infinity and
          ib_options[:stream_full_chunks] === true and
          check_ibworker_proc(ibworker) do
        {:ibrowse_req_id, :mock_response}
      else
        {:error, {:conn_failed, :conn_failed}}
      end
    end)

    {:ok, pid} = ChangesFollower.start_link(MyChangesFollower, {:normal, d})

    :meck.wait(:ibrowse, :send_req_direct, :_, 550)

    # -- Uncomment for recording
    # run_relay(pid, d, {"_changes", %{feed: :continuous, heartbeat: 60_000, timeout: 10_800_000}})
    # -- Uncomment for replay
    run_replay(pid, [
      {:ibrowse_async_headers, :mock_response, '200', [
        {'Transfer-Encoding', 'chunked'},
        {'Server', 'CouchDB/1.6.1 (Erlang OTP/17)'},
        {'Date', 'Sat, 02 Sep 2017 13:48:32 GMT'},
        {'Content-Type', 'application/json'},
        {'Cache-Control', 'must-revalidate'}]},
      {:ibrowse_async_response, :mock_response, ~s({"seq":1,"id":"new_document","changes":[{"rev":"1-946f699664e83aa36fc2833fa634adca"}]}\n)},
      {:ibrowse_async_response, :mock_response, ~s({"seq":3,"id":"deleted_document","changes":[{"rev":"2-eec205a9d413992850a6e32678485900"}],"deleted":true}\n\n\n)},
      {:ibrowse_async_response, :mock_response, ~s({"seq":4,"id":"changed_document","changes":[{"rev":"1-a4ed57fb1018f3d423d0adec26f14dfc"}]}\n)}
    ])
    # -- End

    ChangesFollower.cast(pid, {:cast_test, self()})
    assert_receive :cast_test

    send(pid, {:other_message_test, self()})
    assert_receive :other_message_test

    assert ChangesFollower.whereis(pid) === pid

    assert ChangesFollower.call(pid, :check) === :ok

    assert ChangesFollower.call(pid, {:reply, :timeout}) === :ok
    assert ChangesFollower.call(pid, {:noreply, :timeout}) === :ok

    assert ChangesFollower.stop(pid) === :ok
    refute Process.alive?(pid)

    assert :meck.validate(:ibrowse)

    :meck.unload(:ibrowse)
  end

  test "include_docs and doc_ids" do
    s = ICouch.server_connection("http://192.168.99.100:8000/")
    d = ICouch.DB.new(s, "changes_test_db")

    :meck.expect(:ibrowse, :spawn_worker_process, &start_ibworker_proc/1)
    :meck.expect(:ibrowse, :stop_worker_process, &stop_ibworker_proc/1)
    :meck.expect(:ibrowse, :send_req_direct, fn (ibworker,
        'http://192.168.99.100:8000/changes_test_db/_changes?feed=continuous&filter=_doc_ids&heartbeat=60000&include_docs=true&timeout=10800000',
        [{"Content-Type", "application/json"}, {"Accept", "application/json"}], :post, "{\"doc_ids\":[\"changed_document\"]}", ib_options, 10_805_000) ->
      if length(ib_options) === 4 and
          ib_options[:response_format] === :binary and
          ib_options[:stream_to] === self() and
          ib_options[:stream_chunk_size] === :infinity and
          ib_options[:stream_full_chunks] === true and
          check_ibworker_proc(ibworker) do
        {:ibrowse_req_id, :mock_response}
      else
        {:error, {:conn_failed, :conn_failed}}
      end
    end)

    {:ok, pid} = ChangesFollower.start_link(MyChangesFollower, {:include_docs, d})

    :meck.wait(:ibrowse, :send_req_direct, :_, 550)

    run_replay(pid, [
      {:ibrowse_async_headers, :mock_response, '200', [
        {'Transfer-Encoding', 'chunked'},
        {'Server', 'CouchDB/1.6.1 (Erlang OTP/17)'},
        {'Date', 'Tue, 12 Sep 2017 10:43:12 GMT'},
        {'Content-Type', 'application/json'},
        {'Cache-Control', 'must-revalidate'}]},
      {:ibrowse_async_response, :mock_response, ~s({"seq":4,"id":"changed_document","changes":[{"rev":"1-a4ed57fb1018f3d423d0adec26f14dfc"}],"doc":{"_id":"changed_document","_rev":"1-a4ed57fb1018f3d423d0adec26f14dfc","old":"one"}}\n)}
    ])

    assert ChangesFollower.whereis(pid) === pid

    assert ChangesFollower.call(pid, :check) === :ok

    assert ChangesFollower.stop(pid) === :ok
    refute Process.alive?(pid)

    assert :meck.validate(:ibrowse)

    :meck.unload(:ibrowse)
  end

  test "resilence" do
    s = ICouch.server_connection("http://192.168.99.100:8000/")
    d = ICouch.DB.new(s, "changes_test_db")

    :meck.expect(:ibrowse, :spawn_worker_process, &start_ibworker_proc/1)
    :meck.expect(:ibrowse, :stop_worker_process, 1, :meck.seq([
      :meck.exec(fn (pid) ->
        # Stopping for sel_conn_closed does not generate a closing_on_request
        stop_ibworker_proc(pid)
        receive do
          {:ibrowse_async_response, :mock_response, {:error, :closing_on_request}} -> :ok
        end
      end),
      :meck.exec(&stop_ibworker_proc/1)
    ]))
    :meck.expect(:ibrowse, :send_req_direct, 7, :meck.seq([
      :meck.val({:error, :sel_conn_closed}),
      :meck.exec(fn (ibworker, _, _, _, _, _, _) ->
        send(ibworker, :kill)
        {:ibrowse_req_id, :mock_response}
      end),
      :meck.val({:ibrowse_req_id, :mock_response}),
    ]))

    {:ok, pid} = ChangesFollower.start(MyChangesFollower, {:resilent, d})
    Process.link(pid)

    :meck.wait(1, :ibrowse, :send_req_direct, :_, 100)

    # (sel_conn_closed)

    :meck.wait(1, :ibrowse, :stop_worker_process, :_, 550)
    :meck.wait(2, :ibrowse, :send_req_direct, :_, 550)

    # (Worker goes down)

    :meck.wait(3, :ibrowse, :send_req_direct, :_, 550)

    send(pid, {:ibrowse_async_response_timeout, :mock_response})

    :meck.wait(2, :ibrowse, :stop_worker_process, :_, 550)
    :meck.wait(4, :ibrowse, :send_req_direct, :_, 550)

    send(pid, {:ibrowse_async_response_end, :mock_response})

    :meck.wait(5, :ibrowse, :send_req_direct, :_, 550)

    send(pid, {:ibrowse_async_response, :mock_response, {:error, :random}})

    :meck.wait(6, :ibrowse, :send_req_direct, :_, 550)

    run_replay(pid, [
      {:ibrowse_async_headers, :mock_response, '200', [
        {'Transfer-Encoding', 'chunked'},
        {'Server', 'CouchDB/1.6.1 (Erlang OTP/17)'},
        {'Date', 'Sat, 02 Sep 2017 13:48:32 GMT'},
        {'Content-Type', 'application/json'},
        {'Cache-Control', 'must-revalidate'}]},
      {:ibrowse_async_response, :mock_response, "{\"seq\":4,\"id\":\"changed_document\",\"changes\":[{\"rev\":\"1-a4ed57fb1018f3d423d0adec26f14dfc\"}]}\n"}
    ])

    # (Heartbeat missing)

    :meck.wait(3, :ibrowse, :stop_worker_process, :_, 550)
    :meck.wait(7, :ibrowse, :send_req_direct, :_, 700)

    send(pid, {:ibrowse_async_headers, :mock_response, '400', [{'Server', 'CouchDB/1.6.1 (Erlang OTP/17)'}]})

    :meck.wait(4, :ibrowse, :stop_worker_process, :_, 550)
    :meck.wait(8, :ibrowse, :send_req_direct, :_, 550)

    send(pid, {:ibrowse_async_headers, :mock_response, '500', [{'Server', 'CouchDB/1.6.1 (Erlang OTP/17)'}]})

    assert ChangesFollower.call(pid, :check) === :ok

    assert ChangesFollower.stop(pid) === :ok
    refute Process.alive?(pid)

    assert :meck.validate(:ibrowse)

    :meck.unload(:ibrowse)
  end

  test "unrecoverable" do
    s = ICouch.server_connection("http://192.168.99.100:8000/")
    d = ICouch.DB.new(s, "changes_test_db")

    :meck.expect(:ibrowse, :spawn_worker_process, &start_ibworker_proc/1)
    :meck.expect(:ibrowse, :stop_worker_process, &stop_ibworker_proc/1)
    :meck.expect(:ibrowse, :send_req_direct, 7, :meck.val({:ibrowse_req_id, :mock_response}))

    {:ok, pid} = ChangesFollower.start(MyChangesFollower, {:normal, d})
    Process.monitor(pid)

    :meck.wait(:ibrowse, :send_req_direct, :_, 100)

    send(pid, {:ibrowse_async_headers, :mock_response, '400', [{'Server', 'CouchDB/1.6.1 (Erlang OTP/17)'}]})

    assert_receive {:DOWN, _, :process, ^pid, :bad_request}

    refute Process.alive?(pid)

    {:ok, pid} = ChangesFollower.start(MyChangesFollower, {:normal, d})
    Process.monitor(pid)
    assert ChangesFollower.call(pid, :call_stop) === :ok
    assert_receive {:DOWN, _, :process, ^pid, :call_stop}

    {:ok, pid} = ChangesFollower.start(MyChangesFollower, {:normal, d})
    Process.monitor(pid)
    assert ChangesFollower.cast(pid, :cast_stop) === :ok
    assert_receive {:DOWN, _, :process, ^pid, :cast_stop}

    {:ok, pid} = ChangesFollower.start(MyChangesFollower, {:normal, d})
    Process.monitor(pid)
    :meck.wait(4, :ibrowse, :send_req_direct, :_, 100)
    run_replay(pid, [
      {:ibrowse_async_headers, :mock_response, '200', [
        {'Transfer-Encoding', 'chunked'},
        {'Server', 'CouchDB/1.6.1 (Erlang OTP/17)'},
        {'Date', 'Sat, 02 Sep 2017 13:48:32 GMT'},
        {'Content-Type', 'application/json'},
        {'Cache-Control', 'must-revalidate'}]},
      {:ibrowse_async_response, :mock_response, "{\"seq\":5,\"id\":\"changed_document\",\"changes\":[]}\n{\"seq\":6,\"id\":\"stop_document\",\"changes\":[]}\n"}
    ])
    assert_receive {:DOWN, _, :process, ^pid, :change_stop}

    assert :meck.validate(:ibrowse)

    :meck.unload(:ibrowse)
  end

  # -- Helper functions --

  def run_replay(target, messages) do
    Enum.each(messages, fn (m) ->
      send(target, m)
    end)
  end

  defp ibworker_proc() do
    receive do
      {:stop_ibworker_proc, r} ->
        send(r, {:ibworker_proc, :stop_ibworker_proc, :ok})
        send(r, {:ibrowse_async_response, :mock_response, {:error, :closing_on_request}})
        :ok
      {:check_ibworker_proc, r} ->
        send(r, {:ibworker_proc, :check_ibworker_proc, :ok})
        ibworker_proc()
      :kill ->
        exit(:got_killed)
    end
  end

  defp start_ibworker_proc('http://192.168.99.100:8000/') do
    {:ok, spawn(&ibworker_proc/0)}
  end

  defp check_ibworker_proc(pid) do
    send(pid, {:check_ibworker_proc, self()})
    receive do
      {:ibworker_proc, :check_ibworker_proc, :ok} -> true
    after 100 ->
      false
    end
  end

  defp stop_ibworker_proc(pid) do
    send(pid, {:stop_ibworker_proc, self()})
    receive do
      {:ibworker_proc, :stop_ibworker_proc, :ok} -> :ok
    after 100 ->
      exit(:stop_ibworker_proc)
    end
  end

  # Extra relay/recording function
  def run_relay(target, db, endpoint) do
    r = spawn_link(fn -> relay_proc(target) end)
    {:ibrowse_req_id, _} = ICouch.DB.send_raw_req(
      db, endpoint, :get, nil, [{"Accept", "application/json"}],
      [stream_to: r, stream_chunk_size: :infinity, stream_full_chunks: true])
    on_exit fn ->
      Process.exit(r, :kill)
    end
    :ok
  end

  defp relay_proc(target) do
    receive do
      {:ibrowse_async_headers, _, code, headers} ->
        send(target, IO.inspect({:ibrowse_async_headers, :mock_response, code, headers}))
        relay_proc(target)
      {:ibrowse_async_response, _, chunk} ->
        send(target, IO.inspect({:ibrowse_async_response, :mock_response, chunk}))
        relay_proc(target)
      {:ibrowse_async_response_end, _} ->
        send(target, IO.inspect({:ibrowse_async_response_end, :mock_response}))
        relay_proc(target)
      {:ibrowse_async_response_timeout, _} ->
        send(target, IO.inspect({:ibrowse_async_response_timeout, :mock_response}))
        relay_proc(target)
    end
  end
end
