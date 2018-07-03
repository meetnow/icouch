
# Created by Patrick Schneider on 13.06.2018.
# Copyright (c) 2018 MeetNow! GmbH

defmodule Mix.Tasks.Icouch.Restore do
  use Mix.Task
  require Logger

  import Mix.Tasks.Icouch.Utils

  @backup_header "{\"new_edits\":false,\"docs\":[\n"

  def run(argv) do
    OptionParser.parse(argv, strict: [
      help: :boolean,
      debug: :boolean,
      deleted: :boolean,
      attachments: :boolean,
      timeout: :integer
    ])
    |> case do
      {opts, [source_folder, target_url], []} ->
        if Keyword.get(opts, :help) do
          usage()
        else
          run(source_folder, URI.parse(target_url), opts)
        end
      {opts, [], []} ->
        if !Keyword.get(opts, :help) do
          IO.puts("error: too few arguments\n")
        end
        usage()
      {opts, [_], []} ->
        if !Keyword.get(opts, :help) do
          IO.puts("error: too few arguments\n")
        end
        usage()
      {opts, [_, _ | extra], args} ->
        if !Keyword.get(opts, :help) do
          IO.puts("error: unrecognized arguments: #{join_invalid_args(args, extra)}}\n")
        end
        usage()
      {opts, _, args = [_|_]} ->
        if !Keyword.get(opts, :help) do
          IO.puts("error: unrecognized arguments: #{join_invalid_args(args)}\n")
        end
        usage()
    end
  end

  defp usage() do
    IO.puts("""
    usage: mix icouch.restore [--help] [--no-deleted] [--no-attachments]
                              [--timeout TIMEOUT] source_folder target_url

    positional arguments:
      source_folder Path from which to restore the backup data
      target_url    URL of the target database

    optional arguments:
      --help           Show this help message and exit
      --no-deleted     Ignore deleted documents
      --no-attachments Strip attachments (can't be undone easily)
      --timeout        Request timeout in seconds (defaults to 120)
    """)
  end

  defp run(source_folder, target_url, opts) do
    Application.ensure_all_started(:icouch)

    Logger.configure_backend(:console, [format: "$time $message\n", metadata: [], level: (if opts[:debug], do: :debug, else: :info)])

    timeout = Keyword.get(opts, :timeout, 120) * 1_000
    deleted = Keyword.get(opts, :deleted, true)
    attachments = Keyword.get(opts, :attachments, true)

    bulk_file_path = Path.join(source_folder, "_bulk_docs.json")
    if not File.exists?(bulk_file_path) do
      exit("File not found: #{bulk_file_path}")
    end

    {target_db, target_url} = take_db_name(target_url)
    target_db = ICouch.assert_db!(ICouch.server_connection(target_url, timeout: timeout), target_db)

    Logger.info("Loading documents...")
    {docs_order, docs_by_id, doc_revs} = Enum.reduce(File.stream!(bulk_file_path), nil, fn
      @backup_header, nil ->
        {[], %{}, %{}}
      "\n", acc ->
        acc
      "]\n", acc ->
        acc
      line, {docs_order, docs_by_id, doc_revs} = acc ->
        bsm = max(byte_size(line) - 2, 0)
        %{id: doc_id, rev: doc_rev} = doc = case line do
          <<line1::binary-size(bsm), ",\n">> ->
            ICouch.Document.from_api!(line1)
          _ ->
            ICouch.Document.from_api!(line)
        end
        if not deleted and ICouch.Document.deleted?(doc) do
          acc
        else
          doc = if attachments do
            doc
          else
            ICouch.Document.delete_attachments(doc)
          end
          {[doc_id | docs_order], Map.put(docs_by_id, doc_id, doc), Map.put(doc_revs, doc_id, doc_rev)}
        end
    end)

    case map_size(doc_revs) do
      0 ->
        exit("Backup is empty.")
      n_docs ->
        Logger.info("Read #{n_docs} document(s).")
    end

    docs_order = Enum.reverse(docs_order)

    Logger.info("Checking existing revisions...")
    docs_by_id = ICouch.open_view!(target_db, "_all_docs", keys: docs_order)
      |> ICouch.View.fetch!()
      |> Enum.reduce(docs_by_id, fn
        %{"error" => "not_found"}, acc ->
          acc
        %{"key" => doc_id, "value" => %{"rev" => doc_rev}}, acc ->
          case Map.get(doc_revs, doc_id) do
            ^doc_rev ->
              Map.delete(acc, doc_id)
            nil ->
              acc
            other_rev ->
              Logger.info("Differing revision for document '#{doc_id}': #{doc_rev} -> #{other_rev}")
              Map.delete(acc, doc_id)
          end
      end)

    docs_order = Enum.filter(docs_order, &Map.has_key?(docs_by_id, &1))

    case map_size(docs_by_id) do
      0 ->
        Logger.info("No documents left.")
      n_docs ->
        {{docs_w_atts, docs_wo_atts}, atts_count, atts_size} = if attachments do
          {docs_w_atts, atts_count, atts_size} = Enum.reduce(docs_by_id, {MapSet.new(), 0, 0}, fn
            {doc_id, doc}, {dwa, atts_count, atts_size} = acc ->
              case doc["_attachments"] do
                nil ->
                  acc
                m when map_size(m) == 0 ->
                  acc
                doc_atts ->
                  Enum.reduce(doc_atts, {MapSet.put(dwa, doc_id), atts_count, atts_size}, fn
                    {_, %{"length" => att_size}}, {dwa, atts_count, atts_size} ->
                      {dwa, atts_count + 1, atts_size + att_size}
                  end)
              end
          end)
          {Enum.split_with(docs_order, &MapSet.member?(docs_w_atts, &1)), atts_count, atts_size}
        else
          {{[], docs_order}, 0, 0}
        end

        case docs_wo_atts do
          [] ->
            nil
          _ ->
            Logger.info("#{n_docs} document(s) left.")
            Logger.info("Bulk uploading #{length docs_wo_atts} document(s) without attachments...")
            {:ok, _} = ICouch.save_docs(target_db, for doc_id <- docs_wo_atts do docs_by_id[doc_id] end, new_edits: false)
        end

        case docs_w_atts do
          [] ->
            nil
          _ ->
            n_docs = length(docs_w_atts)
            Logger.info("#{n_docs} document(s) with #{atts_count} attachment(s) of #{human_bytesize(atts_size)} left.")
            Enum.reduce(docs_w_atts, 1, fn doc_id, i_docs ->
              doc_rev = doc_revs[doc_id]
              Logger.info("- #{progress_string(i_docs, n_docs)} #{doc_id} @ #{doc_rev}")
              doc_folder = Path.join(source_folder, doc_id)
              doc = docs_by_id[doc_id]
              doc = Enum.reduce(doc["_attachments"], doc, fn {att_name, _}, doc ->
                ICouch.Document.put_attachment_data(doc, att_name, File.read!(Path.join(doc_folder, att_name)))
              end)
              ICouch.save_doc!(target_db, doc, new_edits: false)
              i_docs + 1
            end)
        end
    end

    Logger.info("All done.")
  end
end
