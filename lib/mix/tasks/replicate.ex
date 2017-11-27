defmodule Mix.Tasks.Icouch.Replicate do
  use Mix.Task

  def run(argv) do
    OptionParser.parse(argv, strict: [
      help: :boolean,
      precheck: :boolean,
      continuous: :boolean,
      since: :string,
      batch_limit: :integer,
      deleted: :boolean
    ])
    |> case do
      {opts, [source_url, target_url], []} ->
        if Keyword.get(opts, :help) do
          usage()
        else
          run(URI.parse(source_url), URI.parse(target_url), opts)
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

  defp join_invalid_args(args, extra \\ []) do
    Enum.map(args, fn {k, nil} -> k; {k, v} -> "#{k} #{v}" end) ++ extra
      |> Enum.join(" ")
  end

  defp usage() do
    IO.puts("""
    usage: mix icouch.replicate [--help] [--no-precheck] [--continuous]
                                [--since SINCE] [--batch-limit BATCH_LIMIT]
                                [--no-deleted] source_url target_url

    positional arguments:
      source_url    URL of the source database
      target_url    URL of the target database

    optional arguments:
      --help        Show this help message and exit
      --no-precheck Do not check existing documents before replicating
      --continuous  Start a live replication
      --since       Choose a different starting point
      --batch-limit Maximum size of batch requests in MB (defaults to 32)
      --no-deleted  Do not replicate document deletions
    """)
  end

  defp take_db_name(%URI{path: path} = uri) do
    case path |> String.split("/") |> Enum.reverse() do
      [d, ""] ->
        {d, %{uri | path: "/"}}
      [d | t] ->
        {d, %{uri | path: t |> Enum.reverse() |> Enum.join("/")}}
    end
  end

  defp run(source_url, target_url, opts) do
    Application.ensure_all_started(:icouch)

    Logger.configure_backend(:console, [format: "$time $message\n", metadata: []])

    opts = case Keyword.pop(opts, :batch_limit) do
      {nil, _} -> opts
      {batch_limit, ropts} -> Keyword.put(ropts, :max_byte_size, batch_limit * 0x100000)
    end

    {source_db, source_url} = take_db_name(source_url)
    source_db = ICouch.open_db!(ICouch.server_connection(source_url, timeout: 120_000), source_db)

    {target_db, target_url} = take_db_name(target_url)
    target_db = ICouch.assert_db!(ICouch.server_connection(target_url, timeout: 120_000), target_db)

    {:ok, pid} = ICouch.Replicator.start_link(source_db, target_db, opts)
    ref = Process.monitor(pid)
    receive do
      {:DOWN, ^ref, _, _, _} ->
        :ok
    end
  end
end
