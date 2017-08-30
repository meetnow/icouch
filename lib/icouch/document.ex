
# Created by Patrick Schneider on 23.08.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule ICouch.Document do
  @moduledoc """
  Module which handles CouchDB documents.

  The struct's main use is to to conveniently handle attachments. You can
  access the fields transparently with `doc["fieldname"]` since this struct
  implements the Elixir Access behaviour. You can also retrieve or pattern match
  the internal document with the `fields` attribute.

  Note that you should not manipulate the fields directly, especially the
  attachments. This is because of the efficient multipart transmission which
  requires the attachments to be in the same order within the JSON as in the
  multipart body. The accessor functions provided in this module handle this.
  """

  @behaviour Access

  defstruct [id: nil, rev: nil, fields: %{}, attachment_order: [], attachment_data: %{}]

  @type key :: String.t
  @type value :: any

  @type t :: %__MODULE__{
    id: String.t | nil,
    rev: String.t | nil,
    fields: %{optional(key) => value},
    attachment_order: [String.t],
    attachment_data: %{optional(key) => binary}
  }

  @doc """
  Creates a new document struct.

  You can create an empty document or give it an ID and revision number or
  create a document struct from a map.
  """
  @spec new(fields :: map) :: t
  @spec new(id :: String.t | nil, rev :: String.t | nil) :: t
  def new(id \\ nil, rev \\ nil)

  def new(fields, _) when is_map(fields),
    do: from_api!(fields)
  def new(nil, nil),
    do: %__MODULE__{}
  def new(id, nil),
    do: %__MODULE__{id: id, fields: %{"_id" => id}}
  def new(id, rev),
    do: %__MODULE__{id: id, rev: rev, fields: %{"_id" => id, "_rev" => rev}}

  @doc """
  Deserializes a document from a map or binary.

  If attachments with data exist, they will be decoded and stored in the struct
  separately.
  """
  @spec from_api(fields :: map | binary) :: {:ok, t} | :error
  def from_api(doc) when is_binary(doc) do
    case Poison.decode(doc) do
      {:ok, fields} -> from_api(fields)
      other -> other
    end
  end
  def from_api(%{"_attachments" => doc_atts} = fields) when map_size(doc_atts) > 0 do
    {doc_atts, order, atts} = Enum.reduce(doc_atts, {doc_atts, [], %{}}, fn ({name, att}, {doc_atts, order, atts}) ->
      case Map.pop(att, "data") do
        {nil, %{"stub" => true}} ->
          {doc_atts, [name | order], atts}
        {nil, _} ->
          att = att
            |> Map.delete("follows")
            |> Map.put("stub", true)
          {%{doc_atts | name => att}, [name | order], atts}
        {data, att_wod} ->
          case Base.decode64(data) do
            {:ok, data} ->
              att = att_wod
                |> Map.put("stub", true)
                |> Map.put("length", byte_size(data))
              {%{doc_atts | name => att}, [name | order], Map.put(atts, name, data)}
            _ ->
              {doc_atts, [name | order], atts}
          end
      end
    end)
    {:ok, %__MODULE__{id: Map.get(fields, "_id"), rev: Map.get(fields, "_rev"), fields: %{fields | "_attachments" => doc_atts}, attachment_order: Enum.reverse(order), attachment_data: atts}}
  end
  def from_api(%{} = fields),
    do: {:ok, %__MODULE__{id: Map.get(fields, "_id"), rev: Map.get(fields, "_rev"), fields: fields}}

  @doc """
  Like `from_api/1` but raises an error on decoding failures.
  """
  @spec from_api!(fields :: map | binary) :: t
  def from_api!(doc) when is_binary(doc),
    do: from_api!(Poison.decode!(doc))
  def from_api!(fields) when is_map(fields) do
    {:ok, doc} = from_api(fields)
    doc
  end

  @doc """
  Deserializes a document including its attachment data from a list of multipart
  segments as returned by `ICouch.Multipart.split/2`.
  """
  @spec from_multipart(parts :: [{map, binary}]) :: {:ok, t} | {:error, term}
  def from_multipart([{doc_headers, doc_body} | atts]) do
    doc_body = case Map.get(doc_headers, "content-encoding") do
      "gzip" ->
        try do
          :zlib.gunzip(doc_body)
        rescue _ ->
          doc_body
        end
      _ ->
        doc_body
    end
    case from_api(doc_body) do
      {:ok, doc} ->
        {:ok, Enum.reduce(atts, doc, fn ({att_headers, att_body}, acc) ->
          case Regex.run(~r/attachment; *filename="([^"]*)"/, Map.get(att_headers, "content-disposition", "")) do
            [_, filename] ->
              case Map.get(att_headers, "content-encoding") do
                "gzip" ->
                  try do
                    :zlib.gunzip(att_body)
                  rescue _ ->
                    nil
                  end
                _ ->
                  att_body
              end
              |>
              case do
                nil -> acc
                att_body -> ICouch.Document.put_attachment_data(acc, filename, att_body)
              end
            _ ->
              acc
          end
        end)}
      other ->
        other
    end
  end

  @doc """
  Serializes the given document to a binary.

  This will encode the attachments unless `multipart: true` is given as option
  at which point the attachments that have data are marked with
  `"follows": true`. The attachment order is maintained.
  """
  @spec to_api(doc :: t, options :: [any]) :: {:ok, binary} | {:error, term}
  def to_api(doc, options \\ []),
    do: Poison.encode(doc, options)

  @doc """
  Like `to_api/1` but raises an error on encoding failures.
  """
  @spec to_api!(doc :: t, options :: [any]) :: binary
  def to_api!(doc, options \\ []),
    do: Poison.encode!(doc, options)

  @doc """
  Serializes the given document including its attachment data to a list of
  multipart segments as consumed by `ICouch.Multipart.join/2`.
  """
  @spec to_multipart(doc :: t) :: {:ok, [{map, binary}]} | :error
  def to_multipart(doc) do
    case to_api(doc, multipart: true) do
      {:ok, doc_body} ->
        {:ok, [
          {%{"Content-Type" => "application/json"}, doc_body} |
          (for {filename, data} <- get_attachment_data(doc), data != nil, do: {%{"Content-Disposition" => "attachment; filename=\"#{filename}\""}, data})
        ]}
      other ->
        other
    end
  end

  @doc """
  Returns whether the document is marked as deleted.
  """
  @spec deleted?(doc :: t) :: boolean
  def deleted?(%__MODULE__{fields: %{"_deleted" => true}}),
    do: true
  def deleted?(%__MODULE__{}),
    do: false

  @doc """
  Marks the document as deleted.

  This removes all fields except `_id` and `_rev` and deletes all attachments
  unless `keep_fields` is set to `true`.
  """
  @spec set_deleted(doc :: t, keep_fields :: boolean) :: t
  def set_deleted(doc, keep_fields \\ false)

  def set_deleted(doc, true),
    do: put(doc, "_deleted", true)
  def set_deleted(%__MODULE__{id: nil, rev: nil} = doc, _),
    do: %{doc | fields: %{"_deleted" => true}, attachment_order: [], attachment_data: %{}}
  def set_deleted(%__MODULE__{id: nil, rev: rev} = doc, _),
    do: %{doc | fields: %{"_rev" => rev, "_deleted" => true}, attachment_order: [], attachment_data: %{}}
  def set_deleted(%__MODULE__{id: id, rev: nil} = doc, _),
    do: %{doc | fields: %{"_id" => id, "_deleted" => true}, attachment_order: [], attachment_data: %{}}
  def set_deleted(%__MODULE__{id: id, rev: rev} = doc, _),
    do: %{doc | fields: %{"_id" => id, "_rev" => rev, "_deleted" => true}, attachment_order: [], attachment_data: %{}}

  @doc """
  Returns the approximate size of this document if it was encoded in JSON.

  Does not include the attachment data.
  """
  def json_byte_size(%__MODULE__{fields: fields}),
    do: ICouch.json_byte_size(fields)

  @doc """
  Set the document ID for `doc`.

  Attempts to set it to `nil` will actually remove the ID from the document.
  """
  @spec set_id(doc :: t, id :: String.t) :: t
  def set_id(%__MODULE__{fields: fields} = doc, nil),
    do: %{doc | id: nil, fields: Map.delete(fields, "_id")}
  def set_id(%__MODULE__{fields: fields} = doc, id),
    do: %{doc | id: id, fields: Map.put(fields, "_id", id)}

  @doc """
  Set the revision number for `doc`.

  Attempts to set it to `nil` will actually remove the revision number from the
  document.
  """
  @spec set_rev(doc :: t, rev :: String.t) :: t
  def set_rev(%__MODULE__{fields: fields} = doc, nil),
    do: %{doc | rev: nil, fields: Map.delete(fields, "_rev")}
  def set_rev(%__MODULE__{fields: fields} = doc, rev),
    do: %{doc | rev: rev, fields: Map.put(fields, "_rev", rev)}

  @doc """
  Fetches the field value for a specific `key` in the given `doc`.

  If `doc` contains the given `key` with value `value`, then `{:ok, value}` is
  returned. If `doc` doesn't contain `key`, `:error` is returned.

  Part of the Access behavior.
  """
  @spec fetch(doc :: t, key) :: {:ok, value} | :error
  def fetch(%__MODULE__{id: id}, "_id"),
    do: if id != nil, do: {:ok, id}, else: :error
  def fetch(%__MODULE__{rev: rev}, "_rev"),
    do: if rev != nil, do: {:ok, rev}, else: :error
  def fetch(%__MODULE__{fields: fields}, key),
    do: Map.fetch(fields, key)

  @doc """
  Gets the field value for a specific `key` in `doc`.

  If `key` is present in `doc` with value `value`, then `value` is
  returned. Otherwise, `default` is returned (which is `nil` unless
  specified otherwise).

  Part of the Access behavior.
  """
  @spec get(doc :: t, key, default :: value) :: value
  def get(doc, key, default \\ nil) do
    case fetch(doc, key) do
      {:ok, value} -> value
      :error -> default
    end
  end

  @doc """
  Gets the field value from `key` and updates it, all in one pass.

  `fun` is called with the current value under `key` in `doc` (or `nil` if `key`
  is not present in `doc`) and must return a two-element tuple: the "get" value
  (the retrieved value, which can be operated on before being returned) and the
  new value to be stored under `key` in the resulting new document. `fun` may
  also return `:pop`, which means the current value shall be removed from `doc`
  and returned (making this function behave like `Document.pop(doc, key)`.

  The returned value is a tuple with the "get" value returned by
  `fun` and a new document with the updated value under `key`.

  Part of the Access behavior.
  """
  @spec get_and_update(doc :: t, key, (value -> {get, value} | :pop)) :: {get, t} when get: term
  def get_and_update(doc, key, fun) when is_function(fun, 1) do
    current = get(doc, key)
    case fun.(current) do
      {get, update} ->
        {get, put(doc, key, update)}
      :pop ->
        {current, delete(doc, key)}
      other ->
        raise "the given function must return a two-element tuple or :pop, got: #{inspect(other)}"
    end
  end

  @doc """
  Returns and removes the field value associated with `key` in `doc`.

  If `key` is present in `doc` with value `value`, `{value, new_doc}` is
  returned where `new_doc` is the result of removing `key` from `doc`. If `key`
  is not present in `doc`, `{default, doc}` is returned.
  """
  @spec pop(doc :: t, key, default :: value) :: {value, t}
  def pop(doc, key, default \\ nil)

  def pop(%__MODULE__{id: id} = doc, "_id", default),
    do: {id || default, set_id(doc, nil)}
  def pop(%__MODULE__{rev: rev} = doc, "_rev", default),
    do: {rev || default, set_rev(doc, nil)}
  def pop(%__MODULE__{fields: fields} = doc, "_attachments", default),
    do: {Map.get(fields, "_attachments", default), delete_attachments(doc)}
  def pop(%__MODULE__{fields: fields} = doc, key, default) do
    {value, fields} = Map.pop(fields, key, default)
    {value, %{doc | fields: fields}}
  end

  @doc """
  Puts the given field `value` under `key` in `doc`.
  """
  @spec put(doc :: t, key, value) :: t
  def put(doc, "_id", value),
    do: set_id(doc, value)
  def put(doc, "_rev", value),
    do: set_rev(doc, value)
  def put(_, "_attachments", _),
    do: raise ArgumentError, message: "attachment changing not yet supported through this function"
  def put(%__MODULE__{fields: fields} = doc, key, value),
    do: %{doc | fields: Map.put(fields, key, value)}

  @doc """
  Deletes the entry in `doc` for a specific `key`.

  If the `key` does not exist, returns `doc` unchanged.
  """
  @spec delete(doc :: t, key) :: t
  def delete(doc, "_id"),
    do: set_id(doc, nil)
  def delete(doc, "_rev"),
    do: set_rev(doc, nil)
  def delete(doc, "_attachments"),
    do: delete_attachments(doc)
  def delete(%__MODULE__{fields: fields} = doc, key),
    do: %{doc | fields: :maps.remove(key, fields)}

  @doc """
  Returns the attachment info (stub) for a specific `filename` or `nil` if not
  found.
  """
  @spec get_attachment_info(doc :: t, filename :: key) :: map | nil
  def get_attachment_info(%__MODULE__{fields: fields}, filename),
    do: Map.get(fields, "_attachments", %{}) |> Map.get(filename)

  @doc """
  Returns whether an attachment with the given `filename` exists.
  """
  @spec has_attachment?(doc :: t, filename :: key) :: map | nil
  def has_attachment?(%__MODULE__{fields: %{"_attachments" => doc_atts}}, filename),
    do: Map.has_key?(doc_atts, filename)
  def has_attachment?(%__MODULE__{}, _),
    do: false

  @doc """
  Returns the data of the attachment specified by `filename` if present or
  `nil`.
  """
  @spec get_attachment_data(doc :: t, filename :: key) :: binary | nil
  def get_attachment_data(%__MODULE__{attachment_data: data}, filename),
    do: Map.get(data, filename)

  @doc """
  Returns whether the attachment specified by `filename` is present and has
  data associated with it.
  """
  @spec has_attachment_data?(doc :: t, filename :: key) :: boolean
  def has_attachment_data?(%__MODULE__{attachment_data: data}, filename),
    do: Map.has_key?(data, filename)

  @doc """
  Returns a list of tuples with attachment names and data, in the order in which
  they will appear in the serialized JSON.

  Note that the list will also contain attachments that have no associated data.
  """
  @spec get_attachment_data(doc :: t) :: [{String.t, binary | nil}]
  def get_attachment_data(%__MODULE__{attachment_order: order, attachment_data: data}),
    do: for filename <- order, do: {filename, Map.get(data, filename)}

  @doc """
  Returns both the info and data of the attachment specified by `filename` if
  present or `nil`. The data itself can be missing.
  """
  @spec get_attachment(doc :: t, filename :: key) :: {map, binary | nil} | nil
  def get_attachment(%__MODULE__{fields: fields, attachment_data: data}, filename) do
    case Map.get(fields, "_attachments", %{}) |> Map.get(filename) do
      nil ->
        nil
      attachment_info ->
        {attachment_info, Map.get(data, filename)}
    end
  end

  @doc """
  Inserts or updates the attachment info (stub) in `doc` for the attachment
  specified by `filename`.
  """
  @spec put_attachment_info(doc :: t, filename :: key, info :: map) :: t
  def put_attachment_info(%__MODULE__{fields: fields, attachment_order: order} = doc, filename, info) do
    case Map.get(fields, "_attachments", %{}) do
      %{^filename => _} = doc_atts ->
        %{doc | fields: Map.put(fields, "_attachments", %{doc_atts | filename => info})}
      doc_atts ->
        %{doc | fields: Map.put(fields, "_attachments", Map.put(doc_atts, filename, info)), attachment_order: order ++ [filename]}
    end
  end

  @doc """
  Inserts or updates the attachment data in `doc` for the attachment specified
  by `filename`. The attachment info (stub) has to exist or an error is raised.
  """
  @spec put_attachment_data(doc :: t, filename :: key, data :: binary) :: t
  def put_attachment_data(%__MODULE__{fields: %{"_attachments" => doc_atts}, attachment_data: attachment_data} = doc, filename, data) do
    if not Map.has_key?(doc_atts, filename), do: raise KeyError, key: filename, term: "_attachments"
    %{doc | attachment_data: Map.put(attachment_data, filename, data)}
  end
  def put_attachment_data(%__MODULE__{}, filename, _),
    do: raise KeyError, key: filename, term: "_attachments"

  @doc """
  Deletes the attachment data of all attachments in `doc`.

  This does not delete the respective attachment info (stub).
  """
  @spec delete_attachment_data(doc :: t) :: t
  def delete_attachment_data(%__MODULE__{} = doc),
    do: %{doc | attachment_data: %{}}

  @doc """
  Deletes the attachment data specified by `filename` from `doc`.
  """
  @spec delete_attachment_data(doc :: t, filename :: key) :: t
  def delete_attachment_data(%__MODULE__{attachment_data: attachment_data} = doc, filename),
    do: %{doc | attachment_data: Map.delete(attachment_data, filename)}

  @doc """
  """
  @spec put_attachment(doc :: t, filename :: key, data :: binary | {map | binary}, content_type :: String.t, digest :: String.t | nil) :: t
  def put_attachment(doc, filename, data, content_type \\ "application/octet-stream", digest \\ nil)

  def put_attachment(doc, filename, data, content_type, nil) when is_binary(data),
    do: put_attachment(doc, filename, {%{"content_type" => content_type, "length" => byte_size(data), "stub" => true}, data}, "", nil)
  def put_attachment(doc, filename, data, content_type, digest) when is_binary(data),
    do: put_attachment(doc, filename, {%{"content_type" => content_type, "digest" => digest, "length" => byte_size(data), "stub" => true}, data}, "", nil)
  def put_attachment(%__MODULE__{fields: fields, attachment_order: order, attachment_data: attachment_data} = doc, filename, {stub, data}, _, _) do
    {doc_atts, order} = case Map.get(fields, "_attachments", %{}) do
      %{^filename => _} = doc_atts ->
        {Map.put(doc_atts, filename, stub), order}
      doc_atts ->
        {Map.put(doc_atts, filename, stub), order ++ [filename]}
    end
    %{doc | fields: Map.put(fields, "_attachments", doc_atts), attachment_order: order, attachment_data: Map.put(attachment_data, filename, data)}
  end

  @doc """
  Deletes the attachment specified by `filename` from `doc`.

  Returns the document unchanged, if the attachment didn't exist.
  """
  @spec delete_attachment(doc :: t, filename :: key) :: t
  def delete_attachment(%__MODULE__{fields: %{"_attachments" => doc_atts} = fields, attachment_order: order, attachment_data: data} = doc, filename),
    do: %{doc | fields: %{fields | "_attachments" => Map.delete(doc_atts, filename)}, attachment_order: List.delete(order, filename), attachment_data: Map.delete(data, filename)}
  def delete_attachment(%__MODULE__{} = doc, _),
    do: doc

  @doc """
  Deletes all attachments and attachment data from `doc`.
  """
  @spec delete_attachments(doc :: t) :: t
  def delete_attachments(%__MODULE__{fields: fields} = doc),
    do: %{doc | fields: Map.delete(fields, "_attachments"), attachment_order: [], attachment_data: %{}}

  @doc """
  Returns the size of the attachment data specified by `filename` in `doc`.

  Note that this will return 0 if the attachment data is missing and/or the
  attachment does not exist.
  """
  @spec attachment_data_size(doc :: t, filename :: key) :: integer
  def attachment_data_size(%__MODULE__{attachment_data: data}, filename),
    do: byte_size(Map.get(data, filename, ""))

  @doc """
  Returns the sum of all attachment data sizes in `doc`.

  The calculation is done for data that actually is present in this document,
  not neccessarily all attachments that are referenced in `_attachments`.
  """
  @spec attachment_data_size(doc :: t) :: integer
  def attachment_data_size(%__MODULE__{attachment_data: data}),
    do: Enum.reduce(data, 0, fn ({_, d}, acc) -> acc + byte_size(d) end)
end

defimpl Enumerable, for: ICouch.Document do
  def count(%ICouch.Document{fields: fields}),
    do: {:ok, map_size(fields)}

  def member?(%ICouch.Document{fields: fields}, {key, value}),
    do: {:ok, match?({:ok, ^value}, :maps.find(key, fields))}
  def member?(_doc, _other),
    do: {:ok, false}

  def reduce(%ICouch.Document{fields: fields}, acc, fun),
    do: Enumerable.Map.reduce(fields, acc, fun)
end

defimpl Poison.Encoder, for: ICouch.Document do
  @compile :inline_list_funcs

  alias Poison.Encoder

  use Poison.{Encode, Pretty}

  def encode(%ICouch.Document{fields: %{"_attachments" => doc_atts}, attachment_order: []}, _) when map_size(doc_atts) > 0,
    do: raise ArgumentError, message: "document attachments inconsistent"
  def encode(%ICouch.Document{fields: %{"_attachments" => doc_atts} = fields, attachment_order: [_|_] = order, attachment_data: data}, options) do
    if length(order) != map_size(doc_atts),
      do: raise ArgumentError, message: "document attachments inconsistent"

    shiny = pretty(options)
    if shiny do
      indent = indent(options)
      offset = offset(options) + indent
      options = offset(options, offset)

      fun = &[",\n", spaces(offset), Encoder.BitString.encode(encode_name(&1), options), ": ",
              encode_field_value(&1, :maps.get(&1, fields), order, data, shiny, options) | &2]
      ["{\n", tl(:lists.foldl(fun, [], :maps.keys(fields))), ?\n, spaces(offset - indent), ?}]
    else
      fun = &[?,, Encoder.BitString.encode(encode_name(&1), options), ?:,
              encode_field_value(&1, :maps.get(&1, fields), order, data, shiny, options) | &2]
      [?{, tl(:lists.foldl(fun, [], :maps.keys(fields))), ?}]
    end
  end
  def encode(%ICouch.Document{fields: fields}, options),
    do: Poison.Encoder.Map.encode(fields, options)

  defp encode_field_value("_attachments", doc_atts, order, data, true, options) do
    multipart = Keyword.get(options, :multipart, false)
    indent = indent(options)
    offset = offset(options) + indent
    options = offset(options, offset)

    fun = &[",\n", spaces(offset), Encoder.BitString.encode(encode_name(&1), options), ": ",
            encode_attachment(:maps.get(&1, doc_atts), :maps.find(&1, data), multipart, options) | &2]
    ["{\n", tl(:lists.foldr(fun, [], order)), ?\n, spaces(offset - indent), ?}]  
  end
  defp encode_field_value("_attachments", doc_atts, order, data, _, options) do
    multipart = Keyword.get(options, :multipart, false)
    fun = &[?,, Encoder.BitString.encode(encode_name(&1), options), ?:,
            encode_attachment(:maps.get(&1, doc_atts), :maps.find(&1, data), multipart, options) | &2]
    [?{, tl(:lists.foldr(fun, [], order)), ?}]
  end
  defp encode_field_value(_, value, _, _, _, options),
    do: Encoder.encode(value, options)

  defp encode_attachment(att, {:ok, _}, true, options),
    do: Encoder.Map.encode(att |> Map.delete("stub") |> Map.put("follows", true), options)
  defp encode_attachment(att, {:ok, data}, _, options),
    do: Encoder.Map.encode(att |> Map.delete("stub") |> Map.delete("length") |> Map.put("data", Base.encode64(data)), options)
  defp encode_attachment(att, _, _, options),
    do: Encoder.Map.encode(att, options)
end
