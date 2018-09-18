
# Created by Patrick Schneider on 29.03.2018.
# Copyright (c) 2018 MeetNow! GmbH

# Planned features:
# --keep           Keep deleted attachments on disk

defmodule Mix.Tasks.Icouch.Backup do
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
      {opts, [source_url, target_folder], []} ->
        if Keyword.get(opts, :help) do
          usage()
        else
          run(URI.parse(source_url), target_folder, opts)
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
    usage: mix icouch.backup [--help] [--no-attachments] [--timeout TIMEOUT]
                             source_url target_folder

    positional arguments:
      source_url    URL of the source database
      target_folder Path to store the backup data

    optional arguments:
      --help           Show this help message and exit
      --no-deleted     Ignore deleted documents
      --no-attachments Do not download attachments
      --timeout        Request timeout in seconds (defaults to 120)
    """)
  end

  defp stream_attachment(ref, file, size) do
    receive do
      %ICouch.StreamChunk{ref: ^ref, data: data} ->
        :ok = :file.write(file, data)
        stream_attachment(ref, file, size + byte_size(data))
      %ICouch.StreamEnd{ref: ^ref} ->
        :ok = File.close(file)
        {:ok, size}
      other ->
        IO.inspect(other)
        stream_attachment(ref, file, size)
    end
  end

  defp store_attachment(source_db, doc_id, doc_rev, att_name, temp_path, target_path, target_size) do
    file = File.open!(temp_path, [:raw, :write])
    {:ok, ref} = ICouch.stream_attachment(source_db, doc_id, att_name, self(), rev: doc_rev)
    {:ok, ^target_size} = stream_attachment(ref, file, 0)
    :ok = File.rename(temp_path, target_path)
  end

  defp store_blanky(source_db, doc_id, doc_rev, target_path) do
    doc = ICouch.open_doc!(source_db, doc_id, rev: doc_rev, attachments: true, multipart: false, revs: true)
    File.write!(target_path, Poison.encode!(doc))
  end

  def save_bulk_docs(docs_order, docs_by_id, bulk_file_path) do
    Logger.info("Saving...")
    [first_doc_id | r_docs_order] = Enum.reverse(docs_order)

    [@backup_header, Poison.encode!(docs_by_id[first_doc_id])]
      |> Stream.concat( Stream.flat_map(r_docs_order, &[",\n", Poison.encode!(docs_by_id[&1])]) )
      |> Stream.concat(["\n]}\n"])
      |> Stream.into(File.stream!(bulk_file_path))
      |> Stream.run()
  end

  defp run(source_url, target_folder, opts) do
    Application.ensure_all_started(:icouch)

    Logger.configure_backend(:console, [format: "$time $message\n", metadata: [], level: (if opts[:debug], do: :debug, else: :info)])

    timeout = Keyword.get(opts, :timeout, 120) * 1_000

    {source_db, source_url} = take_db_name(source_url)
    source_db = ICouch.open_db!(ICouch.server_connection(source_url, timeout: timeout), source_db)

    if not File.exists?(target_folder), do: File.mkdir_p!(target_folder)
    dld_temp_path = Path.join(target_folder, ".download.tmp")
    nr_temp_path = Path.join(target_folder, ".new_revisions.tmp")

    bulk_file_path = Path.join(target_folder, "_bulk_docs.json")
    file_stream = File.stream!(bulk_file_path)

    {docs_order, docs_by_id, doc_revs} = if File.exists?(bulk_file_path) do
      Logger.info("Reading existing backup...")
      Enum.reduce(file_stream, nil, fn
        @backup_header, nil ->
          {[], %{}, %{}}
        "\n", acc ->
          acc
        "]}\n", acc ->
          acc
        line, {docs_order, docs_by_id, doc_revs} ->
          bsm = max(byte_size(line) - 2, 0)
          %{id: doc_id, rev: doc_rev} = doc = case line do
            <<line1::binary-size(bsm), ",\n">> ->
              ICouch.Document.from_api!(line1)
            _ ->
              ICouch.Document.from_api!(line)
          end
          {[doc_id | docs_order], Map.put(docs_by_id, doc_id, doc), Map.put(doc_revs, doc_id, doc_rev)}
      end)
    else
      {[], %{}, %{}}
    end

    {docs_order, docs_by_id, doc_revs} = if File.exists?(nr_temp_path) do
      Logger.info("Reading revision download temp file...")
      File.stream!(nr_temp_path)
        |> Enum.reduce({docs_order, docs_by_id, doc_revs}, fn
          line, {docs_order, docs_by_id, doc_revs} = acc ->
            case ICouch.Document.from_api(binary_part(line, 0, byte_size(line) - 1)) do
              {:ok, %{id: doc_id, rev: doc_rev} = doc} ->
                case Map.get(doc_revs, doc_id) do
                  ^doc_rev ->
                    acc
                  nil ->
                    {[doc_id | docs_order], Map.put(docs_by_id, doc_id, doc), Map.put(doc_revs, doc_id, doc_rev)}
                  _ ->
                    {docs_order, Map.put(docs_by_id, doc_id, doc), Map.put(doc_revs, doc_id, doc_rev)}
                end
              _ ->
                acc
            end
        end)
    else
      {docs_order, docs_by_id, doc_revs}
    end

    case map_size(doc_revs) do
      0 -> nil
      n_docs -> Logger.info("Read #{n_docs} document(s).")
    end

    Logger.info("Loading document list...")

    deleted = Keyword.get(opts, :deleted, true)

    {docs_order, new_doc_revs} = ICouch.open_changes(source_db)
      |> ICouch.Changes.fetch!()
      |> Enum.reduce({docs_order, %{}}, fn row, {docs_order, new_doc_revs} = acc ->
          if deleted or row["deleted"] != true do
            {doc_id, doc_rev} = case row do
              %{"id" => doc_id, "value" => %{"rev" => doc_rev}} -> {doc_id, doc_rev}
              %{"id" => doc_id, "changes" => [%{"rev" => doc_rev}]} -> {doc_id, doc_rev}
            end
            case Map.get(doc_revs, doc_id) do
              ^doc_rev ->
                acc
              nil ->
                {[doc_id | docs_order], Map.put(new_doc_revs, doc_id, doc_rev)}
              _ ->
                {docs_order, Map.put(new_doc_revs, doc_id, doc_rev)}
            end
          else
            acc
          end
      end)

    docs_by_id = case map_size(new_doc_revs) do
      0 ->
        Logger.info("No new revisions.")
        if File.exists?(nr_temp_path) do
          save_bulk_docs(docs_order, docs_by_id, bulk_file_path)
          File.rm!(nr_temp_path)
        end

        docs_by_id

      n_revs ->
        Logger.info("Downloading #{n_revs} new revision(s)...")

        nr_temp = File.open!(nr_temp_path, [:write, :append])
        {docs_by_id, _} = Enum.reduce(new_doc_revs, {docs_by_id, 1}, fn {doc_id, doc_rev}, {acc, i_revs} ->
          Logger.info("- #{progress_string(i_revs, n_revs)} #{doc_id} @ #{doc_rev}")
          doc = ICouch.open_doc!(source_db, doc_id, rev: doc_rev, revs: true)
          IO.binwrite(nr_temp, Poison.encode!(doc) <> "\n")
          {Map.put(acc, doc_id, doc), i_revs + 1}
        end)
        File.close(nr_temp)

        save_bulk_docs(docs_order, docs_by_id, bulk_file_path)
        File.rm!(nr_temp_path)

        docs_by_id
    end

    if Keyword.get(opts, :attachments, true) do
      {atts, atts_count, atts_size} = Enum.reduce(docs_by_id, {[], 0, 0}, fn
        {doc_id, doc}, acc ->
          case doc["_attachments"] do
            nil ->
              acc
            m when map_size(m) == 0 ->
              acc
            doc_atts ->
              doc_rev = doc.rev
              doc_folder = Path.join(target_folder, doc_id)
              Enum.reduce(doc_atts, acc, fn
                {att_name, %{"length" => att_size}}, {atts, atts_count, atts_size} = acc ->
                  if not File.exists?(Path.join(doc_folder, att_name)) do
                    {[{doc_id, doc_rev, att_name, att_size} | atts], atts_count + 1, atts_size + att_size}
                  else
                    acc
                  end
              end)
          end
      end)

      if atts_count > 0 do
        Logger.info("Downloading #{atts_count} attachment(s) of #{human_bytesize(atts_size)}...")
        Enum.reduce(atts, 1, fn
          {doc_id, doc_rev, "", _}, i_atts ->
            doc_folder = Path.join(target_folder, "_all_docs")
            if not File.exists?(doc_folder), do: File.mkdir_p!(doc_folder)
            Logger.info("+ #{progress_string(i_atts, atts_count)} #{doc_id} (Blanky!)")
            store_blanky(source_db, doc_id, doc_rev, Path.join(doc_folder, "#{doc_id}.json"))
            i_atts + 1
          {doc_id, doc_rev, att_name, att_size}, i_atts ->
            doc_folder = Path.join(target_folder, doc_id)
            if not File.exists?(doc_folder), do: File.mkdir_p!(doc_folder)
            Logger.info("+ #{progress_string(i_atts, atts_count)} #{doc_id}/#{att_name} @ #{human_bytesize(att_size)}")
            store_attachment(source_db, doc_id, doc_rev, att_name, dld_temp_path, Path.join(doc_folder, att_name), att_size)
            i_atts + 1
        end)
      else
        Logger.info("No attachments to download.")
      end
    end

    Logger.info("All done.")
  end
end
