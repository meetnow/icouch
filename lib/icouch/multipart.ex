
# Created by Patrick Schneider on 22.08.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule ICouch.Multipart do
  @moduledoc """
  Module for handling multipart streams.

  Used internally, but can be used in other applications.
  """

  @min_chunk_size 1024

  @doc """
  Reads the subtype and boundary from a multipart/* content type header, if found.
  """
  @spec get_boundary(headers :: [{charlist, charlist} | {binary, binary}] | %{optional(String.t) => String.t}) :: {:ok, subtype :: String.t, boundary :: String.t} | nil
  def get_boundary(headers) when is_list(headers) do
    Enum.find_value(headers, fn
      ({'Content-Type', value}) when is_list(value) ->
        extract_boundary_content_type(List.to_string(value))
      ({'content-type', value}) when is_list(value) ->
        extract_boundary_content_type(List.to_string(value))
      ({"Content-Type", value}) when is_binary(value) ->
        extract_boundary_content_type(value)
      ({"content-type", value}) when is_binary(value) ->
        extract_boundary_content_type(value)
      (_) ->
        false
    end)
  end
  def get_boundary(%{"Content-Type" => value}),
    do: extract_boundary_content_type(value)
  def get_boundary(%{"content-type" => value}),
    do: extract_boundary_content_type(value)
  def get_boundary(_),
    do: nil

  defp extract_boundary_content_type(content_type) do
    case Regex.run(~r/multipart\/([^;, ]+) *; *boundary="([^"]*)"/, content_type) do
      [_, subtype, boundary] ->
        {:ok, subtype, boundary}
      _ ->
        nil
    end
  end

  @doc """
  Splits the incoming data up if a boundary was found. Also expects and extracts
  the header.

  Return values:
  * `previous` - data which belongs to the previous part, ready to be used; can be empty
  * `next_headers` - if not nil, a new part has been found; can be an empty map
  * `rest` - data which might belong to the next part or is uncertain; nil when an end boundary has been found
  """
  @spec split_part(data :: binary, boundary :: binary) :: {previous :: binary, next_headers :: map | nil, rest :: binary | nil}
  def split_part(data, boundary) when byte_size(data) < byte_size(boundary) + 4,
    do: {"", nil, data}
  def split_part(data, boundary) do
    bs = byte_size(boundary)
    case data do # Search for boundary
      <<"--", ^boundary :: binary-size(bs), rest :: binary>> ->
        ["", rest]
      _ ->
        :binary.split(data, ["\r\n--" <> boundary, "\n--" <> boundary])
    end
    |>
    case do # Check for end or header begin
      [ending, <<"--", _ :: binary>>] ->
        ending
      [previous, <<"\r\n", following :: binary>> = trailer] ->
        {previous, trailer, following}
      [previous, <<"\n", following :: binary>> = trailer] ->
        {previous, trailer, following}
      _ ->
        nil
    end
    |>
    case do # Find header
      {"", _, ""} ->
        {"", nil, data}
      {previous, trailer, ""} ->
        {previous, nil, <<"--", boundary :: binary, trailer :: binary>>}
      {previous, _, <<"\r\n", following :: binary>>} ->
        {previous, %{}, following}
      {previous, _, <<"\n", following :: binary>>} ->
        {previous, %{}, following}
      {previous, trailer, following} ->
        case :binary.split(following, ["\r\n\r\n", "\n\n"]) do
          [header, rest] ->
            headers = Map.new(for line <- :binary.split(header, "\n", [:global]), String.contains?(line, ":") do
              [key, value] = :binary.split(line, ":")
              {String.downcase(String.trim(key)), String.trim(value)}
            end)
            {previous, headers, rest}
          _ when byte_size(previous) == 0 ->
            {"", nil, data}
          _ ->
            {previous, nil, <<"--", boundary :: binary, trailer :: binary>>}
        end
      other ->
        other
    end
    |>
    case do # In case no boundary + header is found, chop off some data
      nil ->
        diff = byte_size(data) - bs - 5
        if diff >= @min_chunk_size do
          {binary_part(data, 0, diff), nil, binary_part(data, diff, byte_size(data) - diff)}
        else
          {"", nil, data}
        end
      ending when is_binary(ending) ->
        {ending, nil, nil}
      other ->
        other
    end
  end

  @doc """
  Splits an entire multipart stream into its parts.

  Returns error if the stream is incomplete.
  """
  @spec split(data :: binary, boundary :: binary) :: {:ok, parts :: [{headers :: map, body :: binary}]} | :error
  def split(data, boundary),
    do: split(data, boundary, nil, nil)

  defp split(data, boundary, nil, _) do
    case split_part(data, boundary) do
      {_, nil, nil} ->
        {:ok, []}
      {_, nil, _} ->
        :error
      {_, next_headers, rest} ->
        split(rest, boundary, next_headers, [])
    end
  end
  defp split(data, boundary, headers, acc) do
    case split_part(data, boundary) do
      {previous, nil, nil} ->
        {:ok, Enum.reverse([{headers, previous} | acc])}
      {_, nil, _} ->
        :error
      {previous, next_headers, rest} ->
        split(rest, boundary, next_headers, [{headers, previous} | acc])
    end
  end

  @doc """
  Joins a part to form a chunk of multipart stream data.

  According to specification, each part ends with a line break after the data.
  """
  @spec join_part(headers :: %{optional(String.t) => String.t} | [{String.t | atom, String.t}] | nil, data :: binary | nil, boundary :: binary) :: binary
  def join_part(nil, nil, boundary),
    do: "--#{boundary}--"
  def join_part(headers, data, boundary) when headers === %{} or headers === [] or headers === nil,
    do: "--#{boundary}\r\n\r\n#{data}\r\n"
  def join_part(headers, data, boundary),
    do: Enum.reduce(headers, "--#{boundary}\r\n", fn ({key, value}, acc) -> "#{acc}#{key}: #{value}\r\n" end) <> "\r\n#{data}\r\n"

  @doc """
  Joins all given parts into one multipart stream.
  """
  @spec join(parts :: [{headers :: %{optional(String.t) => String.t} | [{String.t, String.t}], data :: binary} | binary], boundary :: binary) :: binary
  def join([], boundary),
    do: join_part(nil, nil, boundary)
  def join(parts, boundary) do
    Enum.reduce(parts, "", fn
      ({headers, data}, acc) -> acc <> join_part(headers, data, boundary)
      (data, acc) -> acc <> join_part(%{}, data, boundary)
    end) <> join_part(nil, nil, boundary)
  end
end
