
# Created by Patrick Schneider on 23.11.2017.
# Copyright (c) 2017 MeetNow! GmbH

alias ICouch.{Document, Collection, Changes, View}

# Internal module for live replication; forwards changes to the Replicator module
defmodule ICouch.LiveReplicator do
  @moduledoc false

  use ChangesFollower

  def start_link(db, opts),
    do: ChangesFollower.start_link(__MODULE__, [db, opts, self()])

  def init([db, opts, pid]),
    do: {:ok, db, opts, pid}

  def handle_change(change, pid) do
    GenServer.cast(pid, {:live_change, change})
    {:ok, pid}
  end
end

defmodule ICouch.Replicator do
  @moduledoc """
  Optimized "software" replicator for CouchDB.

  Implements a `GenServer` for replicating databases. It chooses the best
  strategy per document, resulting as little calls and throughput as possible
  while not overcrowding batch requests.

  The status can be queried while the replicator is running.
  """

  @default_max_byte_size 0x2000000 # 32 MB
  @buffer_time 15_000

  use GenServer
  require Logger

  defstruct [
    :status,
    :live,
    :source_db,
    :target_db,
    :opts,
    :max_byte_size,
    :changes,
    :last_seq,
    :nchanges,
    :ichange,
    :seq,
    :collection,
    :live_changes,
    :live_seq,
    :timer
  ]

  def start_link(source_db, target_db, opts, gen_opts \\ []) do
    GenServer.start_link(__MODULE__, [source_db, target_db, opts], gen_opts)
  end

  def status(pid),
    do: GenServer.call(pid, :status)

  # --

  def init([source_db, target_db, opts]) do
    {seq, opts} = Keyword.pop(opts, :since)
    {max_byte_size, opts} = Keyword.pop(opts, :max_byte_size, @default_max_byte_size)
    if Keyword.get(opts, :continuous, false) do
      Logger.info "Following live changes#{if seq == nil, do: "", else: " since "}#{if seq != nil, do: seq, else: ""}..."
      {:ok, _} = ICouch.LiveReplicator.start_link(source_db, include_docs: true, since: seq)
      {:ok, %__MODULE__{status: :fetching, live: true, source_db: source_db, target_db: target_db, opts: opts, live_changes: Collection.new(), max_byte_size: max_byte_size, seq: seq}}
    else
      send(self(), :fetch)
      {:ok, %__MODULE__{status: :fetching, live: false, source_db: source_db, target_db: target_db, opts: opts, max_byte_size: max_byte_size, seq: seq}}
    end
  end

  def handle_call(:status, _from, s) do
    {:reply, Map.take(s, [:status, :live, :last_seq, :nchanges, :ichange, :seq]), s}
  end

  def handle_cast({:live_change, %{"doc" => doc, "seq" => seq} = change}, %__MODULE__{opts: opts, live_changes: live_changes} = s) do
    if not Keyword.get(opts, :deleted, true) and Map.get(change, "deleted", false) do
      {:noreply, s}
    else
      live_changes = Collection.put(live_changes, {doc, {seq, :all}})
      {:noreply, start_timer(%{s | live_changes: live_changes, live_seq: seq})}
    end
  end

  def handle_info(:fetch, %__MODULE__{live: false, source_db: source_db, opts: opts, seq: seq} = s) do
    Logger.info "Fetching changes#{if seq == nil, do: "", else: " since "}#{if seq != nil, do: seq, else: ""}..."
    changes = %{last_seq: last_seq} = ICouch.open_changes(source_db, include_docs: true, since: seq)
      |> Changes.fetch!()

    changes = if not Keyword.get(opts, :deleted, true) do
      Enum.filter(changes, fn %{"deleted" => true} -> false; _ -> true end)
    else
      changes
    end
      |> Enum.into(Collection.new(), fn %{"doc" => doc, "seq" => seq} -> {doc, {seq, :all}} end)

    fetch_finished(changes, last_seq, s)
  end
  def handle_info(:fetch, %__MODULE__{live: true, live_changes: changes, live_seq: last_seq} = s) do
    if not Collection.empty?(changes) do
      fetch_finished(changes, last_seq, %{s | live_changes: Collection.new(), live_seq: nil, timer: nil})
    else
      {:noreply, %{s | timer: nil}}
    end
  end
  def handle_info(:ex_fetch, %__MODULE__{target_db: target_db, changes: changes} = s) do
    Logger.info "Fetching existing documents..."
    {changes, n} = ICouch.open_view!(target_db, "_all_docs", keys: Collection.doc_ids(changes), include_docs: true)
      |> View.fetch!()
      |> Enum.reduce({changes, 0}, fn
        %{"error" => _}, acc ->
          acc
        %{"id" => doc_id, "value" => %{"rev" => doc_rev, "deleted" => true}, "doc" => nil}, {changes, n} = acc ->
          case Collection.pop(changes, doc_id) do
            {%{rev: new_doc_rev} = new_doc, rchanges} ->
              if doc_rev == new_doc_rev do
                if not Document.deleted?(new_doc) do
                  Logger.info "- Document #{doc_id} appears deleted, but should not be"
                end
                {rchanges, n + 1}
              else
                rev_cp = compare_revs(doc_rev, new_doc_rev)
                cond do
                  rev_cp > 0 ->
                    Logger.debug "- Deleted document #{doc_id} found with higher revision: #{doc_rev} > #{new_doc_rev}"
                    {rchanges, n + 1}
                  rev_cp == 0 ->
                    Logger.warn "- Deleted document #{doc_id} found with conflicting revision: #{doc_rev} <> #{new_doc_rev}"
                    {rchanges, n + 1}
                  true ->
                    acc # No further optimization possible
                end
              end
            _ ->
              acc
          end
        %{"id" => doc_id, "value" => %{"rev" => doc_rev}, "doc" => doc}, {changes, n} = acc ->
          case Collection.pop_meta(changes, doc_id) do
            {nil, _} ->
              acc
            {{%{rev: new_doc_rev} = new_doc, {seq, _}}, rchanges} ->
              if doc_rev == new_doc_rev do
                if not Document.equal?(new_doc, doc) do
                  Logger.info "- Document #{doc_id} exists with same revision, but is not equal"
                end
                {rchanges, n + 1}
              else
                rev_cp = compare_revs(doc_rev, new_doc_rev)
                cond do
                  rev_cp > 0 ->
                    Logger.debug "- Document #{doc_id} found with higher revision: #{doc_rev} > #{new_doc_rev}"
                    {rchanges, n + 1}
                  rev_cp == 0 ->
                    Logger.warn "- Document #{doc_id} found with conflicting revision: #{doc_rev} <> #{new_doc_rev}"
                    {rchanges, n + 1}
                  true ->
                    # Heavy attachment optimization
                    case find_changed_attachments(new_doc, doc) do
                      :all ->
                        acc
                      changed ->
                        {Collection.put(rchanges, new_doc, {seq, changed}), n}
                    end
                end
              end
          end
      end)

    if n == 0 do
      Logger.info "No equal documents found."
    else
      Logger.info "Removed #{n} existing document(s)."
    end

    prepare_run(changes, s)
  end
  def handle_info(:next, %__MODULE__{live: false, changes: [], collection: collection} = s) do
    if not Collection.empty?(collection) do
      upload_collection(collection, true, s)
    end
    Logger.info "All done."
    {:stop, :normal, %{s | status: :done, changes: nil}}
  end
  def handle_info(:next, %__MODULE__{live: true, changes: [], last_seq: last_seq, collection: collection, live_changes: live_changes} = s) do
    if not Collection.empty?(collection) do
      upload_collection(collection, true, s)
    end
    Logger.info "Finished up to seq: #{last_seq}"
    s = %{s | status: :fetching, changes: nil, nchanges: nil, ichange: nil, seq: last_seq, collection: nil}
    {:noreply, (if Collection.empty?(live_changes), do: s, else: start_timer(s))}
  end
  def handle_info(:next, %__MODULE__{source_db: source_db, target_db: target_db, max_byte_size: max_bs, changes: [{doc_id, doc_rev, {seq, changed}} | t], nchanges: nchanges, ichange: ichange, collection: collection} = s) do
    Logger.info "[#{ichange}/#{nchanges} #{:erlang.float_to_binary(ichange * 100 / nchanges, decimals: 2)}%] #{doc_id} | #{doc_rev} | #{seq}"

    collection = case changed do
      :all ->
        ICouch.open_doc(source_db, doc_id, rev: doc_rev, revs: true, attachments: true)
      [] ->
        ICouch.open_doc(source_db, doc_id, rev: doc_rev, revs: true)
      _ ->
        case ICouch.open_doc(source_db, doc_id, rev: doc_rev, revs: true) do
          {:ok, doc} ->
            {:ok, Enum.reduce(changed, doc, fn
              name, doc -> Document.put_attachment_data(doc, name, ICouch.fetch_attachment!(source_db, doc_id, name, rev: doc_rev))
            end)}
          other ->
            other
        end
    end
    |>
    case do
      {:ok, doc} ->
        if Document.full_json_byte_size(doc) > max_bs do
          Logger.debug "Notice: Document size (#{Document.json_byte_size(doc) |> human_bytesize()} + #{Document.attachment_data_size(doc) |> human_bytesize()}) exceeds batch limit; sending directly"
          case ICouch.save_doc(target_db, doc, new_edits: false) do
            {:ok, _} -> nil
            {:error, reason} -> Logger.error "Error while writing document #{doc_id}: #{inspect reason}"
          end
          Collection.delete(collection, doc_id)
        else
          Collection.put(collection, doc)
        end
      {:error, reason} ->
        Logger.error "Download failed: #{reason}"
        collection
    end

    send(self(), :next)
    upload_collection(collection, false, %{s | changes: t, ichange: ichange + 1, seq: seq})
  end
  def handle_info(:upload_collection, %__MODULE__{collection: collection} = s) do
    if not Collection.empty?(collection) do
      upload_collection(collection, true, s)
    else
      {:noreply, s}
    end
  end

  defp fetch_finished(changes, last_seq, %__MODULE__{opts: opts} = s) do
    Logger.info "Retrieved #{Collection.count(changes)} change(s). Size without attachments: #{Collection.byte_size(changes) |> human_bytesize()}"

    if Keyword.get(opts, :precheck, true) do
      send(self(), :ex_fetch)
      {:noreply, %{s | status: :ex_fetching, changes: changes, last_seq: last_seq}}
    else
      prepare_run(changes, %{s | last_seq: last_seq})
    end
  end

  defp prepare_run(changes, %__MODULE__{live: live, last_seq: last_seq, live_changes: live_changes} = s) do
    if Collection.empty?(changes) do
      if not live do
        Logger.info "Nothing to do."
        {:stop, :normal, %{s | status: :done, changes: nil}}
      else
        Logger.info "Finished up to seq: #{last_seq}"
        s = %{s | status: :fetching, changes: nil, nchanges: nil, ichange: nil, seq: last_seq, collection: nil}
        {:noreply, (if Collection.empty?(live_changes), do: s, else: start_timer(s))}
      end
    else
      send(self(), :next)
      {:noreply, %{s | status: :replicating, changes: Collection.doc_revs_meta(changes), nchanges: Collection.count(changes), ichange: 1, collection: Collection.new()}}
    end
  end

  defp upload_collection(collection, false, %__MODULE__{max_byte_size: max_bs} = s) do
    if Collection.byte_size(collection) > max_bs do
      upload_collection(collection, true, s)
    else
      {:noreply, %{s | collection: collection}}
    end
  end
  defp upload_collection(collection, true, %__MODULE__{target_db: target_db} = s) do
    Logger.debug "Batch uploading #{Collection.count(collection)} document(s) of #{Collection.byte_size(collection) |> human_bytesize()}..."
    case ICouch.save_docs(target_db, Collection.to_list(collection), new_edits: false) do
      {:ok, []} -> nil
      {:ok, result} -> Logger.warn "Unexpected result from batch upload: #{inspect result}"
      {:error, reason} -> Logger.error "Error on batch upload: #{inspect reason}"
    end
    {:noreply, %{s | collection: Collection.new()}}
  end

  defp human_bytesize(s) when s < 1024,
    do: "#{s} B"
  defp human_bytesize(s),
    do: human_bytesize(s / 1024, ["KiB","MiB","GiB","TiB"])
  defp human_bytesize(s, [_ | [_|_] = l]) when s >= 1024,
    do: human_bytesize(s / 1024, l)
  defp human_bytesize(s, [m | _]),
    do: "#{:erlang.float_to_binary(s, decimals: 1)} #{m}"

  defp find_changed_attachments(%Document{id: doc_id, fields: %{"_attachments" => atts1}}, %Document{fields: %{"_attachments" => atts2}} = doc) when map_size(atts2) > 0 do
    Enum.reduce(atts1, {[], 0, 0}, fn
      {name, %{"digest" => dig1} = meta1}, {changed, m, n} when dig1 != nil ->
        case Document.get_attachment_info(doc, name) do
          %{"digest" => dig2} = meta2 when dig2 != nil ->
            if meta1["content_type"] == meta2["content_type"] and dig1 == dig2 and meta1["length"] == meta2["length"] do
              {changed, m, n + 1}
            else
              {[name | changed], m + 1, n + 1}
            end
          _ ->
            {[name | changed], m + 1, n + 1}
        end
      {name, _}, {changed, m, n} ->
        {[name | changed], m + 1, n + 1}
    end)
    |>
    case do
      {_, n, n} ->
        :all
      {[], _, _} ->
        Logger.debug "- All attachments unchanged: #{doc_id}"
        []
      {changed, m, n} ->
        Logger.debug "- #{m} of #{n} attachment(s) changed: #{doc_id}"
        changed
    end
  end
  defp find_changed_attachments(_, _),
    do: :all

  defp compare_revs(rev1, rev2) do
    [rp1, _] = String.split(rev1, "-", parts: 2)
    [rp2, _] = String.split(rev2, "-", parts: 2)
    rp1i = String.to_integer(rp1)
    rp2i = String.to_integer(rp2)
    if rp1i < rp2i do
      -1
    else
      if rp1i > rp2i, do: 1, else: 0
    end
  end

  defp start_timer(%{status: :fetching, live: true, timer: nil} = s),
    do: %{s | timer: Process.send_after(self(), :fetch, @buffer_time)}
  defp start_timer(s),
    do: s
end
