
# Created by Patrick Schneider on 13.06.2018.
# Copyright (c) 2018 MeetNow! GmbH

defmodule Mix.Tasks.Icouch.Utils do
  def join_invalid_args(args, extra \\ []) do
    Enum.map(args, fn {k, nil} -> k; {k, v} -> "#{k} #{v}" end) ++ extra
      |> Enum.join(" ")
  end

  def take_db_name(%URI{path: path} = uri) do
    case path |> String.split("/") |> Enum.reverse() do
      [d, ""] ->
        {d, %{uri | path: "/"}}
      [d | t] ->
        {d, %{uri | path: t |> Enum.reverse() |> Enum.join("/")}}
    end
  end

  def human_bytesize(s) when s < 1024,
    do: "#{s} B"
  def human_bytesize(s),
    do: human_bytesize(s / 1024, ["KiB","MiB","GiB","TiB"])
  def human_bytesize(s, [_ | [_|_] = l]) when s >= 1024,
    do: human_bytesize(s / 1024, l)
  def human_bytesize(s, [m | _]),
    do: "#{:erlang.float_to_binary(s, decimals: 1)} #{m}"

  def progress_string(i, n),
    do: "[#{i}/#{n} #{:erlang.float_to_binary(i * 100 / n, decimals: 2)}%]"
end
