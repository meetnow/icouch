
# Created by Patrick Schneider on 22.11.2017.
# Copyright (c) 2017 MeetNow! GmbH

alias ICouch.Document

defmodule ICouch.Collection do
  @moduledoc """
  Collection of documents useful for batch tasks.

  This module helps collecting documents together for batch uploading. It keeps
  track of the document byte sizes to allow a size cap and supports replacing a
  document already in the collection. Furthermore it is possible to associate
  meta data with documents (e.g. a sequence number).

  There are implementations for the `Enumerable` and `Collectable` protocol for
  your convenience.

  All functions with the `_meta` suffix return the document and meta data as
  2-tuple, unless otherwise specified.
  """

  @behaviour Access

  defstruct [
    contents: %{},
    byte_size: 0
  ]

  @type doc_id :: String.t
  @type doc_rev :: String.t | nil
  @type meta :: term

  @type t :: %__MODULE__{
    contents: %{optional(doc_id) => {Document.t, meta}},
    byte_size: integer
  }

  @doc """
  Initialize a collection struct.
  """
  @spec new() :: t
  def new(), do: %__MODULE__{}

  @doc """
  Returns whether the collection is empty.
  """
  @spec empty?(coll :: t) :: boolean
  def empty?(%__MODULE__{contents: contents}),
    do: map_size(contents) == 0

  @doc """
  Returns the number of documents in the collection.
  """
  @spec count(coll :: t) :: integer
  def count(%__MODULE__{contents: contents}),
    do: map_size(contents)

  @doc """
  Returns the byte size of all documents in the collection as if they were
  encoded entirely in JSON (including attachments in Base64).
  """
  @spec byte_size(coll :: t) :: integer
  def byte_size(%__MODULE__{byte_size: byte_size}),
    do: byte_size

  @doc """
  Returns all document IDs as list.
  """
  def doc_ids(%__MODULE__{contents: contents}),
    do: Map.keys(contents)

  @doc """
  Returns a list of tuples pairing document IDs and their rev values.
  """
  @spec doc_revs(coll :: t) :: [{doc_id, doc_rev}]
  def doc_revs(%__MODULE__{contents: contents}),
    do: Enum.map(contents, fn {doc_id, {%Document{rev: rev}, _}} -> {doc_id, rev} end)

  @doc """
  Returns a list of tuples with document IDs, their rev values and metadata.
  """
  @spec doc_revs_meta(coll :: t) :: [{doc_id, doc_rev, meta}]
  def doc_revs_meta(%__MODULE__{contents: contents}) do
    for {doc_id, {%Document{rev: rev}, meta}} <- contents, do: {doc_id, rev, meta}
  end

  @doc """
  Returns all documents as list.
  """
  @spec to_list(coll :: t) :: [Document.t]
  def to_list(%__MODULE__{contents: contents}) do
    for {doc, _} <- Map.values(contents), do: doc
  end

  @doc """
  Returns all documents and meta data as list of tuples.
  """
  @spec to_list_meta(coll :: t) :: [Document.t]
  def to_list_meta(%__MODULE__{contents: contents}),
    do: Map.values(contents)

  @doc """
  Puts a document into the collection, replacing an existing document if needed.

  Also accepts a 2-tuple of a document and meta data; meta data is set to `nil`
  if not given.
  """
  @spec put(coll :: t, Document.t | {Document.t, meta}) :: t
  def put(coll, %Document{} = doc),
    do: put(coll, doc, nil)
  def put(coll, {%Document{} = doc, meta}),
    do: put(coll, doc, meta)

  @doc """
  Puts a document with meta data into the collection, replacing an existing
  document and meta data if needed.
  """
  @spec put(coll :: t, Document.t, meta) :: t
  def put(%__MODULE__{contents: contents, byte_size: bs} = coll, %Document{id: doc_id} = doc, meta) do
    bs = Document.full_json_byte_size(doc) + case Map.fetch(contents, doc_id) do
      :error -> bs
      {:ok, {old_doc, _}} -> bs - Document.full_json_byte_size(old_doc)
    end
    %{coll | contents: Map.put(contents, doc_id, {doc, meta}), byte_size: bs}
  end

  @doc """
  Replaces meta data of the document specified by `doc_id` in the collection.

  Warning: if the document does not exist, returns `coll` unchanged.
  """
  @spec set_meta(coll :: t, doc_id, meta) :: t
  def set_meta(coll, doc_id, meta) do
    case Map.fetch(coll, doc_id) do
      :error -> coll
      {:ok, {doc, _}} -> %{coll | doc_id => {doc, meta}}
    end
  end

  @doc """
  Fetches the document of a specific `doc_id` in the given `coll`.

  If `coll` contains the document with the given `doc_id`, then `{:ok, doc}` is
  returned. If `coll` doesn't contain the document, `:error` is returned.

  Part of the Access behavior.
  """
  @spec fetch(coll :: t, doc_id) :: {:ok, Document.t} | :error
  def fetch(%__MODULE__{contents: contents}, doc_id) do
    case Map.fetch(contents, doc_id) do
      {:ok, {doc, _}} -> {:ok, doc}
      :error -> :error
    end
  end

  @doc """
  Fetches the document and its meta data for a specific `doc_id` in the given
  `coll`.

  If `coll` contains the document with the given `doc_id`, then
  `{:ok, {doc, meta}}` is returned. If `coll` doesn't contain the document,
  `:error` is returned.
  """
  @spec fetch_meta(coll :: t, doc_id) :: {:ok, {Document.t, meta}} | :error
  def fetch_meta(%__MODULE__{contents: contents}, doc_id),
    do: Map.fetch(contents, doc_id)

  @doc """
  Gets the document for a specific `doc_id` in `coll`.

  If `coll` contains the document with the given `doc_id`, then the document is
  returned. Otherwise, `default` is returned (which is `nil` unless
  specified otherwise).

  Part of the Access behavior.
  """
  @spec get(coll :: t, doc_id, default :: any) :: Document.t | any
  def get(%__MODULE__{contents: contents}, doc_id, default \\ nil) do
    case Map.fetch(contents, doc_id) do
      {:ok, {doc, _}} -> doc
      :error -> default
    end
  end

  @doc """
  Gets the document and its meta data for a specific `doc_id` in `coll`.

  If `coll` contains the document with the given `doc_id`, then the document and
  its meta data is returned. Otherwise, `default` is returned (which is `nil`
  unless specified otherwise).
  """
  @spec get_meta(coll :: t, doc_id, default) :: {Document.t, meta} | default when default: term
  def get_meta(%__MODULE__{contents: contents}, doc_id, default \\ nil) do
    case Map.fetch(contents, doc_id) do
      {:ok, doc_meta} -> doc_meta
      :error -> default
    end
  end

  @doc """
  Gets the document for a specific `doc_id` and updates it, all in one pass.

  `fun` is called with the current document under `doc_id` in `coll` (or `nil`
  if the document is not present in `coll`) and must return a two-element tuple:
  the "get" document (the retrieved document, which can be operated on before
  being returned) and the new document to be stored under `doc_id` in the
  resulting new collection. `fun` may also return `:pop`, which means the
  document shall be removed from `coll` and returned (making this function
  behave like `Collection.pop(coll, doc_id)`.

  The returned value is a tuple with the "get" document returned by
  `fun` and a new collection with the updated document under `doc_id`.

  Not that it is possible to return a document plus meta data from `fun`.

  Part of the Access behavior.
  """
  @spec get_and_update(coll :: t, doc_id, (Document.t | nil -> {get, Document.t | {Document.t, meta}} | :pop)) :: {get, t} when get: term
  def get_and_update(coll, doc_id, fun) when is_function(fun, 1) do
    current = get(coll, doc_id)
    case fun.(current) do
      {get, update} ->
        {get, put(coll, update)}
      :pop ->
        {current, delete(coll, doc_id)}
      other ->
        raise "the given function must return a two-element tuple or :pop, got: #{inspect(other)}"
    end
  end

  @doc """
  Gets the document and its meta data for a specific `doc_id` and updates it,
  all in one pass.

  `fun` is called with the current document and meta data under `doc_id` in
  `coll` (or `nil` if the document is not present in `coll`) and must return a
  two-element tuple: the "get" meta data (the retrieved meta data, which can be operated on before
  being returned) and the new meta data to be stored under `doc_id` in the
  resulting new collection. `fun` may also return `:pop`, which means the
  document shall be removed from `coll` and its meta data returned (making this
  function behave like `Collection.pop_meta(coll, doc_id)`.

  The returned value is a tuple with the "get" document and meta data returned
  by `fun` and a new collection with the updated document and meta data under
  `doc_id`.
  """
  @spec get_and_update_meta(coll :: t, doc_id, ({Document.t, meta} | nil -> {get, Document.t | {Document.t, meta}} | :pop)) :: {get, t} when get: term
  def get_and_update_meta(coll, doc_id, fun) when is_function(fun, 1) do
    current = get_meta(coll, doc_id)
    case fun.(current) do
      {get, update} ->
        {get, put(coll, update)}
      :pop ->
        {current, delete(coll, doc_id)}
      other ->
        raise "the given function must return a two-element tuple or :pop, got: #{inspect(other)}"
    end
  end

  @doc """
  Returns and removes a document from `coll`, referenced by its ID or itself.

  If the document is present in `coll`, `{doc, new_coll}` is returned where
  `new_coll` is the result of removing the document from `coll`. If the document
  is not present in `coll`, `{default, coll}` is returned.
  """
  @spec pop(coll :: t, key :: doc_id | Document.t, default) :: {Document.t | default, t} when default: term
  def pop(coll, key, default \\ nil)

  def pop(coll, %Document{id: doc_id}, default),
    do: pop(coll, doc_id, default)
  def pop(%__MODULE__{contents: contents, byte_size: bs} = coll, doc_id, default) when is_binary(doc_id) do
    case Map.pop(contents, doc_id) do
      {nil, _} ->
        {default, coll}
      {{doc, _}, contents} ->
        {doc, %{coll | contents: contents, byte_size: bs - Document.full_json_byte_size(doc)}}
    end
  end

  @doc """
  Returns and removes a document and its meta data from `coll`, referenced by
  its ID or itself.

  If the document is present in `coll`, `{{doc, meta}, new_coll}` is returned
  where `new_coll` is the result of removing the document from `coll`. If the
  document is not present in `coll`, `{default, coll}` is returned.
  """
  @spec pop_meta(coll :: t, key :: doc_id | Document.t, default) :: {{Document.t, meta} | default, t} when default: term
  def pop_meta(coll, key, default \\ nil)

  def pop_meta(coll, %Document{id: doc_id}, default),
    do: pop_meta(coll, doc_id, default)
  def pop_meta(%__MODULE__{contents: contents, byte_size: bs} = coll, doc_id, default) when is_binary(doc_id) do
    case Map.pop(contents, doc_id) do
      {nil, _} ->
        {default, coll}
      {{doc, _} = doc_meta, contents} ->
        {doc_meta, %{coll | contents: contents, byte_size: bs - Document.full_json_byte_size(doc)}}
    end
  end

  @doc """
  Deletes a document in `coll`, referenced by its ID or itself.

  If the document does not exist, returns `coll` unchanged.
  """
  @spec delete(coll :: t, key :: doc_id | Document.t) :: t
  def delete(coll, key),
    do: pop(coll, key) |> elem(1)

  @doc """
  Checks if a document exists within the collection.

  If a string is given as parameter, checks if a document with that ID exists.
  If a 2-tuple of two strings is given, the first element is the document ID and
  the second element the revision to check. Otherwise a document or a 2-tuple of
  document and meta data can be given.

  Documents are tested using `ICouch.Document.equal?/2` if they exist. Meta
  data is compared using `==`.
  """
  @spec member?(coll :: t, doc_id | Document.t | {doc_id, doc_rev}) :: boolean
  def member?(%__MODULE__{contents: contents}, %Document{id: doc_id} = doc1) do
    case Map.fetch(contents, doc_id) do
      {:ok, {doc2, _}} -> Document.equal?(doc1, doc2)
      _ -> false
    end
  end
  def member?(%__MODULE__{contents: contents}, {%Document{id: doc_id} = doc1, meta1}) do
    case Map.fetch(contents, doc_id) do
      {:ok, {doc2, meta2}} -> Document.equal?(doc1, doc2) and meta1 == meta2
      _ -> false
    end
  end
  def member?(%__MODULE__{contents: contents}, {doc_id, doc_rev}) when is_binary(doc_id),
    do: match?({:ok, %{rev: ^doc_rev}}, Map.fetch(contents, doc_id))
  def member?(%__MODULE__{contents: contents}, doc_id) when is_binary(doc_id),
    do: Map.has_key?(contents, doc_id)
  def member?(_coll, _other),
    do: false
end

defimpl Enumerable, for: ICouch.Collection do
  def count(%ICouch.Collection{contents: contents}),
    do: {:ok, map_size(contents)}

  def member?(coll, key),
    do: {:ok, ICouch.Collection.member?(coll, key)}

  def slice(%ICouch.Collection{contents: contents}),
    do: Enumerable.Map.slice(contents)

  def reduce(%ICouch.Collection{contents: contents}, acc, fun),
    do: reduce_contents(Map.to_list(contents), acc, fun)

  defp reduce_contents(_, {:halt, acc}, _fun),
    do: {:halted, acc}
  defp reduce_contents(contents, {:suspend, acc}, fun),
    do: {:suspended, acc, &reduce_contents(contents, &1, fun)}
  defp reduce_contents([], {:cont, acc}, _fun),
    do: {:done, acc}
  defp reduce_contents([{doc, _} | t], {:cont, acc}, fun),
    do: reduce_contents(t, fun.(doc, acc), fun)
end

defimpl Collectable, for: ICouch.Collection do
  def into(%ICouch.Collection{contents: start_contents, byte_size: start_size}) do
    {{start_contents, start_size}, fn
      {contents, size}, {:cont, element} ->
        {%Document{id: doc_id} = doc, meta} = case element do
          %Document{} = doc -> {doc, nil}
          other -> other
        end
        size = Document.full_json_byte_size(doc) + case Map.fetch(contents, doc_id) do
          :error -> size
          {:ok, {old_doc, _}} -> size - Document.full_json_byte_size(old_doc)
        end
        {Map.put(contents, doc_id, {doc, meta}), size}
      {contents, size}, :done ->
        %ICouch.Collection{contents: contents, byte_size: size}
      _, :halt ->
        :ok
    end}
  end
end
