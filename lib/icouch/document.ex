
# Created by Patrick Schneider on 23.08.2017.
# Copyright (c) 2017,2018 MeetNow! GmbH

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
  def from_api(%{"_attachments" => doc_atts} = fields) when (is_map(doc_atts) and map_size(doc_atts) > 0) or doc_atts != [] do
    {doc_atts, order, data} = Enum.reduce(doc_atts, {%{}, [], %{}}, &decode_att/2)
    {:ok, %__MODULE__{id: Map.get(fields, "_id"), rev: Map.get(fields, "_rev"), fields: %{fields | "_attachments" => doc_atts}, attachment_order: Enum.reverse(order), attachment_data: data}}
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
    doc_body = if ICouch.Server.has_gzip_encoding?(doc_headers) do
      try do
        :zlib.gunzip(doc_body)
      rescue _ ->
        doc_body
      end
    else
      doc_body
    end
    case from_api(doc_body) do
      {:ok, doc} ->
        {:ok, Enum.reduce(atts, doc, fn {att_headers, att_body}, acc ->
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
  Tests two documents for equality.

  Includes `_id`, `_rev` and `_revisions`/`_revs_info`.
  Attachments are compared using `equal_attachments?/2`.
  """
  @spec equal?(t | map, t | map) :: boolean
  def equal?(%__MODULE__{id: id, rev: rev} = doc1, %__MODULE__{id: id, rev: rev} = doc2) do
    if revisions(doc1) == revisions(doc2),
      do: equal_content?(doc1, doc2),
      else: false
  end
  def equal?(%__MODULE__{}, %__MODULE__{}),
    do: false
  def equal?(doc1, %__MODULE__{} = doc2) when is_map(doc1),
    do: equal?(from_api!(doc1), doc2)
  def equal?(%__MODULE__{} = doc1, doc2) when is_map(doc2),
    do: equal?(doc1, from_api!(doc2))
  def equal?(doc1, doc2) when is_map(doc1) and is_map(doc2),
    do: equal?(from_api!(doc1), from_api!(doc2))

  @doc """
  Tests two documents for field equality.

  Ignores `_id`, `_rev` and `_revisions`.
  Attachments are compared using `equal_attachments?/2`.
  """
  @spec equal_content?(t | map, t | map) :: boolean
  def equal_content?(%__MODULE__{fields: fields1} = doc1, %__MODULE__{fields: fields2} = doc2) do
    dropped = ["_id", "_rev", "_revisions", "_attachments"]
    if Map.drop(fields1, dropped) == Map.drop(fields2, dropped),
      do: equal_attachments?(doc1, doc2),
      else: false
  end
  def equal_content?(doc1, %__MODULE__{} = doc2) when is_map(doc1),
    do: equal_content?(from_api!(doc1), doc2)
  def equal_content?(%__MODULE__{} = doc1, doc2) when is_map(doc2),
    do: equal_content?(doc1, from_api!(doc2))
  def equal_content?(doc1, doc2) when is_map(doc1) and is_map(doc2),
    do: equal_content?(from_api!(doc1), from_api!(doc2))

  @doc """
  Tests the attachments of two documents for equality.

  An attachment is considered equal if the name, content_type, length and digest
  are equal. If digests are absent, the data will be checked for equality; if
  both documents do not hold attachment data, this is considered equal as well.
  """
  @spec equal_attachments?(t | map, t | map) :: boolean
  def equal_attachments?(%__MODULE__{fields: %{"_attachments" => atts1}, attachment_data: data1}, %__MODULE__{fields: %{"_attachments" => atts2}, attachment_data: data2})
      when map_size(atts1) == map_size(atts2) do
    Enum.all?(atts1, fn {name, meta1} ->
      case Map.fetch(atts2, name) do
        {:ok, meta2} ->
          dig1 = meta1["digest"]
          dig2 = meta2["digest"]
          meta1["content_type"] == meta2["content_type"] and (
            (dig1 != nil and dig2 != nil and dig1 == dig2 and meta1["length"] == meta2["length"])
            or
            (dig1 == nil and dig2 == nil and data1[name] == data2[name])
          )
        _ ->
          false
      end
    end)
  end
  def equal_attachments?(%__MODULE__{fields: fields1}, %__MODULE__{fields: fields2}),
    do: not Map.has_key?(fields1, "_attachments") and not Map.has_key?(fields2, "_attachments")
  def equal_attachments?(doc1, %__MODULE__{} = doc2) when is_map(doc1),
    do: equal_attachments?(from_api!(doc1), doc2)
  def equal_attachments?(%__MODULE__{} = doc1, doc2) when is_map(doc2),
    do: equal_attachments?(doc1, from_api!(doc2))
  def equal_attachments?(doc1, doc2) when is_map(doc1) and is_map(doc2),
    do: equal_attachments?(from_api!(doc1), from_api!(doc2))

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
  def set_deleted(%__MODULE__{id: id, rev: rev, fields: %{"_revisions" => revisions}} = doc, _),
    do: %{doc | fields: %{"_id" => id, "_rev" => rev, "_revisions" => revisions, "_deleted" => true}, attachment_order: [], attachment_data: %{}}
  def set_deleted(%__MODULE__{id: id, rev: rev} = doc, _),
    do: %{doc | fields: %{"_id" => id, "_rev" => rev, "_deleted" => true}, attachment_order: [], attachment_data: %{}}

  @doc """
  Returns the approximate size of this document if it was encoded in JSON.

  Does not include the attachment data.
  """
  def json_byte_size(%__MODULE__{fields: fields}),
    do: ICouch.json_byte_size(fields)

  @doc """
  Returns the approximate size of this document if it was encoded entirely in
  JSON, including the attachments being represented as as Base64.
  """
  def full_json_byte_size(doc) do
    atds = attachment_data_size(doc)
    json_byte_size(doc) + div(atds + rem(3 - rem(atds, 3), 3), 3) * 4
  end

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

  The `_attachments` field is treated specially:
  - Any given `data` is decoded internally and the `length` attribute is set
  - The `follows` attribute is removed and the `stub` attribute is set
  - If the attachments are given as list of tuples, their order is preserved

  If the document already had attachments:
  - If `data` is set for any given attachment, it will override existing data;
    if not, existing data is kept
  - If the attachments are given as map, the existing order is preserved;
    if not, the order is taken from the list
  """
  @spec put(doc :: t, key, value) :: t
  def put(doc, "_id", value),
    do: set_id(doc, value)
  def put(doc, "_rev", value),
    do: set_rev(doc, value)
  def put(%__MODULE__{fields: fields} = doc, "_attachments", doc_atts) when (is_map(doc_atts) and map_size(doc_atts) == 0) or doc_atts == [],
    do: %{doc | fields: Map.put(fields, "_attachments", %{}), attachment_order: [], attachment_data: %{}}
  def put(%__MODULE__{fields: fields, attachment_order: orig_order, attachment_data: orig_data} = doc, "_attachments", doc_atts) do
    data = Enum.reduce(Map.keys(orig_data), orig_data, fn name, acc -> if Map.has_key?(doc_atts, name), do: acc, else: Map.delete(acc, name) end)
    {clean_doc_atts, order, data} = Enum.reduce(doc_atts, {%{}, [], data}, &decode_att/2)
    order = if is_map(doc_atts) do
      Enum.reduce(
        order,
        Enum.reduce(orig_order, {[], %{}}, fn
          name, {o, s} = acc -> if Map.has_key?(doc_atts, name), do: {[name | o], Map.put(s, name, true)}, else: acc
        end),
        fn name, {o, s} = acc -> if Map.has_key?(s, name), do: acc, else: {[name | o], s} end
      ) |> elem(0)
    else
      order
    end
    %{doc | fields: Map.put(fields, "_attachments", clean_doc_atts), attachment_order: Enum.reverse(order), attachment_data: data}
  end
  def put(%__MODULE__{fields: fields} = doc, key, value),
    do: %{doc | fields: Map.put(fields, key, value)}

  defp decode_att({name, att}, {doc_atts, order, data}) do
    case Map.pop(att, "data") do
      {nil, %{"stub" => true} = att} ->
        {Map.put(doc_atts, name, att), [name | order], data}
      {nil, att_wod} ->
        att = att_wod
          |> Map.delete("follows")
          |> Map.put("stub", true)
        {Map.put(doc_atts, name, att), [name | order], data}
      {b64_data, att_wod} ->
        case Base.decode64(b64_data) do
          {:ok, dec_data} ->
            att = att_wod
              |> Map.put("stub", true)
              |> Map.put("length", byte_size(dec_data))
            {Map.put(doc_atts, name, att), [name | order], Map.put(data, name, dec_data)}
          _ ->
            {Map.put(doc_atts, name, att), [name | order], data}
        end
    end
  end

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
    do: Enum.reduce(data, 0, fn {_, d}, acc -> acc + byte_size(d) end)

  @doc """
  Returns a list of full revision numbers given through the document's
  `_revisions` or `_revs_info` field, or `nil` if the both fields are missing
  or invalid. The revisions are sorted from newest to oldest.
  """
  @spec revisions(doc :: t) :: [String.t] | nil
  def revisions(%__MODULE__{fields: %{"_revisions" => %{"ids" => ids, "start" => start}}}) do
    l = length(ids)
    s = start - l + 1
    Enum.reverse(ids)
      |> Enum.reduce({[], s}, fn e, {acc, ss} -> {["#{ss}-#{e}" | acc], ss + 1} end)
      |> elem(0)
  end
  def revisions(%__MODULE__{fields: %{"_revs_info" => ri}}),
    do: (for %{"rev" => r} <- ri, do: r)
  def revisions(%__MODULE__{}),
    do: nil
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

defimpl Collectable, for: ICouch.Document do
  def into(%ICouch.Document{fields: original, attachment_order: attachment_order, attachment_data: attachment_data}) do
    {original, fn
      fields, {:cont, {k, v}} ->
        Map.put(fields, k, v)
      %{"_attachments" => doc_atts} = fields, :done when map_size(doc_atts) > 0 ->
        %ICouch.Document{id: Map.get(fields, "_id"), rev: Map.get(fields, "_rev"), fields: fields, attachment_order: attachment_order, attachment_data: attachment_data}
          |> ICouch.Document.put("_attachments", doc_atts)
      fields, :done ->
        %ICouch.Document{id: Map.get(fields, "_id"), rev: Map.get(fields, "_rev"), fields: fields}
      _, :halt ->
        :ok
    end}
  end
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
