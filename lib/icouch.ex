
# Created by Patrick Schneider on 03.06.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule ICouch do
  @moduledoc """
  Main module of ICouch.

  Note: Multipart transmission is the default; you can disable it by setting
  adding option `multipart: false`.
  """

  use ICouch.RequestError

  alias ICouch.Document

  @type open_doc_option ::
    {:attachments, boolean} | {:att_encoding_info, boolean} |
    {:atts_since, [String.t]} | {:conflicts, boolean} |
    {:deleted_conflicts, boolean} | {:latest, boolean} | {:local_seq, boolean} |
    {:meta, boolean} | {:open_revs, [String.t]} | {:rev, String.t} |
    {:revs, boolean} | {:revs_info, boolean} | {:multipart, boolean} |
    {:stream_to, pid}

  @type save_doc_option ::
    {:new_edits, boolean} | {:batch, boolean} | {:multipart, boolean}

  @type copy_delete_doc_option ::
    {:rev, String.t} | {:batch, boolean}

  @type fetch_attachment_option ::
    {:rev, String.t} | {:stream_to, pid}

  @type put_attachment_option ::
    {:rev, String.t} | {:content_type, String.t} | {:content_length, integer}

  @type delete_attachment_option ::
    {:rev, String.t}

  @type ref :: pid | reference

  @doc """
  Creates a server connection.

  Equivalent to `server_connection("http://127.0.0.1:5984")`
  """
  @spec server_connection() :: ICouch.Server.t
  def server_connection(),
    do: server_connection("http://127.0.0.1:5984")

  @doc """
  Creates a server connection.

  Pass either an URI string or `URI` struct. Basic authorization is supported
  via URI `"http://user:pass@example.com:5984/"` or via options.
  Note that omitting the port number will result in port 80 (or 443) as per URI
  standards.

  The given URI acts as base URI for requests; any CouchDB API calls will be
  relative to it. That said, the last path element (usually) should be
  terminated with a `/` due to the way how relative URIs work.
  See (RFC 3986)[http://tools.ietf.org/html/rfc3986#section-5.2].

  Also note that this does not actually establish a connection; use
  `server_info/1` to test connectivity.

  The available options are mostly equivalent to ibrowse's, but use the string
  type where applicable. Not all options are supported. See the `ICouch.Server`
  module for a list.
  """
  @spec server_connection(uri :: String.t | URI.t, options :: [ICouch.Server.option]) :: ICouch.Server.t
  def server_connection(uri, options \\ []),
    do: ICouch.Server.new(uri, options)

  @doc """
  Returns meta information about the server instance.
  """
  @spec server_info(server :: ICouch.Server.t) :: {:ok, map} | {:error, term}
  def server_info(server) do
    ICouch.Server.send_req(server, "")
  end

  @doc """
  Returns a list of all the databases in the CouchDB instance.
  """
  @spec all_dbs(server :: ICouch.Server.t) :: {:ok, [String.t]} | {:error, term}
  def all_dbs(server) do
    ICouch.Server.send_req(server, "_all_dbs")
  end

  @doc """
  Requests one UUID from the CouchDB instance.

  Note that this is not strictly UUID compliant; it will in fact return a 32
  character hexadecimal string.
  """
  @spec get_uuid(server :: ICouch.Server.t) :: {:ok, String.t} | {:error, term}
  def get_uuid(server) do
    case ICouch.Server.send_req(server, "_uuids") do
      {:ok, %{"uuids" => [uuid]}} -> {:ok, uuid}
      {:ok, _} -> {:error, :invalid_response}
      other -> other
    end
  end

  @doc """
  Same as `get_uuid/1` but returns the value directly on success or raises
  an error on failure.
  """
  @spec get_uuid!(server :: ICouch.Server.t) :: String.t
  def get_uuid!(server) do
    req_result_or_raise! get_uuid(server)
  end

  @doc """
  Requests one or more UUIDs from the CouchDB instance.

  Note that this is not strictly UUID compliant; it will in fact return a list
  of 32 character hexadecimal strings.
  """
  @spec get_uuids(server :: ICouch.Server.t, count :: integer) :: {:ok, [String.t]} | {:error, term}
  def get_uuids(server, count) do
    case ICouch.Server.send_req(server, "_uuids?count=#{count}") do
      {:ok, %{"uuids" => uuids}} -> {:ok, uuids}
      {:ok, _} -> {:error, :invalid_response}
      other -> other
    end
  end

  @doc """
  Same as `get_uuids/2` but returns the value directly on success or raises
  an error on failure.
  """
  @spec get_uuids!(server :: ICouch.Server.t, count :: integer) :: [String.t]
  def get_uuids!(server, count) do
    req_result_or_raise! get_uuids(server, count)
  end

  @doc """
  Opens an existing database and returns a handle on success.

  Does check if the database exists.
  """
  @spec open_db(server :: ICouch.Server.t, db_name :: String.t) :: {:ok, ICouch.DB.t} | {:error, term}
  def open_db(server, db_name) do
    ICouch.DB.new(server, db_name) |> ICouch.DB.exists()
  end

  @doc """
  Same as `open_db/2` but returns the database directly on success or raises
  an error on failure.
  """
  @spec open_db!(server :: ICouch.Server.t, db_name :: String.t) :: ICouch.DB.t
  def open_db!(server, db_name) do
    req_result_or_raise! open_db(server, db_name)
  end

  @doc """
  Tests if a database exists.
  """
  @spec db_exists?(server :: ICouch.Server.t, db_name :: String.t) :: boolean
  def db_exists?(server, db_name) do
    case open_db(server, db_name) do
      {:ok, _} -> true
      _ -> false
    end
  end

  @doc """
  Creates a database and returns a handle on success.

  Fails if the database already exists.
  """
  @spec create_db(server :: ICouch.Server.t, db_name :: String.t) :: {:ok, ICouch.DB.t} | {:error, term}
  def create_db(server, db_name) do
    db = ICouch.DB.new(server, db_name)
    case ICouch.DB.send_raw_req(db, "", :put) do
      {:ok, _} -> {:ok, db}
      other -> other
    end
  end

  @doc """
  Same as `create_db/2` but returns the database directly on success or raises
  an error on failure.
  """
  @spec create_db!(server :: ICouch.Server.t, db_name :: String.t) :: ICouch.DB.t
  def create_db!(server, db_name) do
    req_result_or_raise! create_db(server, db_name)
  end

  @doc """
  Opens or creates a database and returns a handle on success.

  Returns an error if the database could not be created if it was missing.

  Implementation note: this tries to create the database first.
  """
  @spec assert_db(server :: ICouch.Server.t, db_name :: String.t) :: {:ok, ICouch.DB.t} | {:error, term}
  def assert_db(server, db_name) do
    case create_db(server, db_name) do
      {:ok, _} = created ->
        created
      {:error, :precondition_failed} ->
        {:ok, ICouch.DB.new(server, db_name)}
      other ->
        other
    end
  end

  @doc """
  Same as `assert_db/2` but returns the database directly on success or raises
  an error on failure.
  """
  @spec assert_db!(server :: ICouch.Server.t, db_name :: String.t) :: ICouch.DB.t
  def assert_db!(server, db_name) do
    req_result_or_raise! assert_db(server, db_name)
  end

  @doc """
  Gets information about the database.
  """
  @spec db_info(db :: ICouch.DB.t) :: {:ok, map} | {:error, term}
  def db_info(db) do
    ICouch.DB.send_req(db, "")
  end

  @doc """
  Deletes the database, and all the documents and attachments contained within
  it.
  """
  @spec delete_db(db :: ICouch.DB.t) :: :ok | {:error, term}
  def delete_db(db) do
    case ICouch.DB.send_raw_req(db, "", :delete) do
      {:ok, _} -> :ok
      other -> other
    end
  end

  @doc """
  Deletes a database by name.
  """
  @spec delete_db(server :: ICouch.Server.t, db_name :: String.t) :: :ok | {:error, term}
  def delete_db(server, db_name) do
    case ICouch.Server.send_raw_req(server, db_name, :delete) do
      {:ok, _} -> :ok
      other -> other
    end
  end

  @doc """
  Tests if a document exists.
  """
  @spec doc_exists?(db :: ICouch.DB.t, doc_id :: String.t, options :: [open_doc_option]) :: boolean
  def doc_exists?(db, doc_id, options \\ []) do
    options = Keyword.delete(options, :stream_to) |> Keyword.delete(:multipart)
    case ICouch.DB.send_raw_req(db, {doc_id, options}, :head) do
      {:ok, _} -> true
      _ -> false
    end
  end

  @doc """
  Retrieves the last revision of the document with the specified id.
  """
  def get_doc_rev(db, doc_id) do
    case ICouch.DB.send_raw_req(db, doc_id, :head) do
      {:ok, {headers, _}} ->
        case Enum.find_value(headers, fn ({key, value}) -> :string.to_lower(key) == 'etag' && Poison.decode(value) end) do
          {:ok, _} = result -> result
          _ -> {:error, :invalid_response}
        end
      other ->
        other
    end
  end

  @doc """
  Opens a document in a database.

  When the option `stream_to` is set, this will start streaming the document
  to the specified process. See `ICouch.StreamChunk` and `ICouch.StreamEnd`. The
  returned value will be the stream reference. If attachments are requested,
  they will be streamed separately, even if the server does not support multipart.
  """
  @spec open_doc(db :: ICouch.DB.t, doc_id :: String.t, options :: [open_doc_option]) :: {:ok, Document.t | ref} | {:error, term}
  def open_doc(db, doc_id, options \\ []) do
    {multipart, options} = Keyword.pop(options, :multipart, true)
    case Keyword.pop(options, :stream_to) do
      {nil, _} when multipart ->
        case ICouch.DB.send_raw_req(db, {doc_id, options}, :get, nil, [{"Accept", "multipart/related, application/json"}]) do
          {:ok, {headers, body}} ->
            case ICouch.Multipart.get_boundary(headers) do
              {:ok, _, boundary} ->
                case ICouch.Multipart.split(body, boundary) do
                  {:ok, [{_, body} | atts]} ->
                    case ICouch.Document.from_api(body) do
                      {:ok, doc} ->
                        {:ok, Enum.reduce(atts, doc, fn ({att_headers, att_body}, acc) ->
                          case Regex.run(~r/attachment; *filename="([^"]*)"/, Map.get(att_headers, "content-disposition", "")) do
                            [_, filename] -> ICouch.Document.put_attachment_data(acc, filename, att_body)
                            _ -> acc
                          end
                        end)}
                      other ->
                        other
                    end
                  _ ->
                    {:error, :invalid_response}
                end
              _ ->
                ICouch.Document.from_api(body)
            end
        end
      {nil, _} ->
        case ICouch.DB.send_req(db, {doc_id, options}) do
          {:ok, data} -> Document.from_api(data)
          other -> other
        end
      {stream_to, options} ->
        tr_pid = ICouch.StreamTransformer.spawn(:document, doc_id, stream_to)
        ICouch.DB.send_raw_req(db, {doc_id, options}, :get, nil, [{"Accept", (if multipart, do: "multipart/related, ", else: "") <> "application/json"}], [stream_to: tr_pid])
          |> setup_stream_translator(tr_pid)
    end
  end

  @doc """
  Same as `open_doc/3` but returns the document directly on success or raises
  an error on failure.
  """
  @spec open_doc!(db :: ICouch.DB.t, doc_id :: String.t, options :: [open_doc_option]) :: map
  def open_doc!(db, doc_id, options \\ []) do
    req_result_or_raise! open_doc(db, doc_id, options)
  end

  @doc """
  Creates a new document or creates a new revision of an existing document.

  If the document does not have an "_id" property, the function will obtain a
  new UUID via `get_uuid/1`.

  Returns the saved document with updated revision on success.
  """
  @spec save_doc(db :: ICouch.DB.t, doc :: map | Document.t, options :: [save_doc_option]) :: {:ok, map} | {:error, term}
  def save_doc(db, doc, options \\ [])

  def save_doc(db, %{id: doc_id, attachment_data: atts} = doc, options) do
    {multipart, options} = Keyword.pop(options, :multipart, true)
    if multipart and map_size(atts) > 0 do
      :crypto.rand_seed()
      boundary = Base.encode16(:erlang.list_to_binary(Enum.map(1..20, fn (_) -> :rand.uniform(256)-1 end)))
      parts = [
        {%{"Content-Type" => "application/json"}, ICouch.Document.to_api!(doc, multipart: true)} |
        (for {filename, data} <- Document.get_attachment_data(doc), data != nil, do: {%{"Content-Disposition" => "attachment; filename=\"#{filename}\""}, data})
      ]
      body = ICouch.Multipart.join(parts, boundary)
      case ICouch.DB.send_raw_req(db, {doc_id, options}, :put, body, [{'Content-Type', "multipart/related; boundary=\"#{boundary}\""}]) do
        {:ok, {_, %{"rev" => new_rev}}} ->
          {:ok, Document.set_rev(doc, new_rev)}
        other ->
          other
      end
    else
      case ICouch.DB.send_req(db, {doc_id, options}, :put, doc) do
        {:ok, %{"rev" => new_rev}} ->
          {:ok, Document.set_rev(doc, new_rev)}
        {:ok, _} ->
          {:error, :invalid_response}
        other ->
          other
      end
    end
  end
  def save_doc(%ICouch.DB{server: server} = db, doc, options) do
    case get_uuid(server) do
      {:ok, doc_id} ->
        save_doc(db, Document.set_id(doc, doc_id), options)
      other ->
        other
    end
  end
  def save_doc(db, doc, options) when is_map(doc),
    do: save_doc(db, Document.new(doc), options)

  @doc """
  Same as `save_doc/3` but returns the updated document directly on success or
  raises an error on failure.
  """
  @spec save_doc!(db :: ICouch.DB.t, doc :: map, options :: [save_doc_option]) :: map
  def save_doc!(db, doc, options \\ []),
    do: req_result_or_raise! save_doc(db, doc, options)

  @doc """
  Creates and updates multiple documents at the same time within a single
  request.
  """
  @spec save_docs(db :: ICouch.DB.t, docs :: [map], options :: [save_doc_option]) :: {:ok, [map]} | {:error, term}
  def save_docs(db, docs, options \\ []) do
    options = Keyword.delete(options, :multipart)
    case options[:new_edits] do
      false ->
        ICouch.DB.send_req(db, "_bulk_docs", :post, %{"docs" => docs, "new_edits" => false})
      _ ->
        ICouch.DB.send_req(db, "_bulk_docs", :post, %{"docs" => docs})
    end
  end

  @doc """
  Duplicates an existing document by copying it to a new UUID obtained via
  `get_uuid/1`.

  Returns a map with the fields `"id"`, `"rev"` and `"ok"` on success.
  """
  @spec dup_doc(db :: ICouch.DB.t, src_doc :: String.t | map, options :: [copy_delete_doc_option]) :: {:ok, map} | {:error, term}
  def dup_doc(%ICouch.DB{server: server} = db, src_doc, options \\ []) do
    case get_uuid(server) do
      {:ok, dest_doc_id} ->
        copy_doc(db, src_doc, dest_doc_id, options)
      other ->
        other
    end
  end

  @doc """
  Copies an existing document to a new or existing document.

  Returns a map with the fields `"id"`, `"rev"` and `"ok"` on success.
  """
  @spec copy_doc(db :: ICouch.DB.t, src_doc :: String.t | map, dest_doc_id :: String.t, options :: [copy_delete_doc_option]) :: {:ok, map} | {:error, term}
  def copy_doc(db, src_doc, dest_doc_id, options \\ []) do
    src_doc_id = case src_doc do
      %{"_id" => src_doc_id} ->
        src_doc_id
      src_doc_id when is_binary(src_doc_id) ->
        src_doc_id
    end
    case ICouch.DB.send_raw_req(db, {src_doc_id, options}, :copy, nil, [{"Destination", dest_doc_id}]) do
      {:ok, {_, body}} -> Poison.decode(body)
      other -> other
    end
  end

  @doc """
  Deletes a document.

  This marks the specified document as deleted by adding a field `_deleted` with
  the value `true`.

  A rev number is required if only the document ID is given instead of the
  document itself.
  """
  @spec delete_doc(db :: ICouch.DB.t, doc :: String.t | map, options :: [copy_delete_doc_option]) :: {:ok, map} | {:error, term}
  def delete_doc(db, doc, options \\ []) do
    {doc_id, options} = case doc do
      %{"_id" => doc_id, "_rev" => doc_rev} ->
        {doc_id, Keyword.put(options, :rev, doc_rev)}
      doc_id when is_binary(doc_id) ->
        {doc_id, options}
    end
    ICouch.DB.send_req(db, {doc_id, options}, :delete)
  end

  @doc """
  Request compaction of the specified database.
  """
  @spec compact(db :: ICouch.DB.t) :: :ok | {:error, term}
  def compact(db) do
    case ICouch.DB.send_req(db, "_compact", :post) do
      {:ok, _} -> :ok
      other -> other
    end
  end

  @doc """
  Compacts the view indexes associated with the specified design document.
  """
  @spec compact(db :: ICouch.DB.t, design_doc :: String.t) :: :ok | {:error, term}
  def compact(db, design_doc) do
    case ICouch.DB.send_req(db, "_compact/#{design_doc}", :post) do
      {:ok, _} -> :ok
      other -> other
    end
  end

  @doc """
  Commits any recent changes to the specified database to disk.
  """
  @spec ensure_full_commit(db :: ICouch.DB.t) :: {:ok, instance_start_time :: integer | nil} | {:error, term}
  def ensure_full_commit(db) do
    case ICouch.DB.send_req(db, "_ensure_full_commit", :post) do
      {:ok, %{"instance_start_time" => instance_start_time}} -> {:ok, instance_start_time}
      {:ok, _} -> {:ok, nil}
      other -> other
    end
  end

  @doc """
  Removes view index files that are no longer required by CouchDB as a result
  of changed views within design documents.
  """
  @spec view_cleanup(db :: ICouch.DB.t) :: :ok | {:error, term}
  def view_cleanup(db) do
    case ICouch.DB.send_req(db, "_view_cleanup", :post) do
      {:ok, _} -> :ok
      other -> other
    end
  end

  @doc """
  Downloads a document attachment.

  When the option `stream_to` is set, this will start streaming the attachment
  to the specified process. See `ICouch.StreamChunk` and `ICouch.StreamEnd`. The
  returned value will be the stream reference.
  """
  @spec fetch_attachment(db :: ICouch.DB.t, doc :: String.t | map, filename :: String.t, options :: [fetch_attachment_option]) :: {:ok, binary | ref} | {:error, term}
  def fetch_attachment(db, doc, filename, options \\ []) do
    doc_id = case doc do
      %{"_id" => doc_id} ->
        doc_id
      doc_id when is_binary(doc_id) ->
        doc_id
    end
    case Keyword.pop(options, :stream_to) do
      {nil, _} ->
        case ICouch.DB.send_raw_req(db, {"#{doc_id}/#{URI.encode(filename)}", options}, :get) do
          {:ok, {_, body}} -> {:ok, body}
          other -> other
        end
      {stream_to, options} ->
        tr_pid = ICouch.StreamTransformer.spawn(:attachment, {doc_id, filename}, stream_to)
        ICouch.DB.send_raw_req(db, {"#{doc_id}/#{URI.encode(filename)}", options}, :get, nil, nil, [stream_to: tr_pid])
          |> setup_stream_translator(tr_pid)
    end
  end

  @doc """
  Uploads a document attachment.

  In order to stream the attachment body, a function of arity 0 or 1 can be
  provided that should return `{:ok, data}` or `:eof`.

  A rev number is required if only the document ID is given instead of the
  document itself.
  """
  @spec put_attachment(db :: ICouch.DB.t, doc :: String.t | map, filename :: String.t, body :: ICouch.Server.body, options :: [put_attachment_option]) :: {:ok, map} | {:error, term}
  def put_attachment(db, doc, filename, body, options \\ []) do
    {doc_id, options} = case doc do
      %{"_id" => doc_id, "_rev" => doc_rev} ->
        {doc_id, Keyword.put(options, :rev, doc_rev)}
      doc_id when is_binary(doc_id) ->
        {doc_id, options}
    end
    {headers, options} = case Keyword.pop(options, :content_type) do
      {nil, options} -> {[], options}
      {content_type, options} -> {[{"Content-Type", content_type}], options}
    end
    {headers, options} = case Keyword.pop(options, :content_length) do
      {nil, _} when is_binary(body) -> {[{"Content-Length", "#{byte_size(body)}"} | headers], options}
      {nil, _} -> {headers, options}
      {content_length, options} -> {[{"Content-Length", "#{content_length}"} | headers], options}
    end
    case ICouch.DB.send_raw_req(db, {"#{doc_id}/#{URI.encode(filename)}", options}, :put, body, headers) do
      {:ok, {_, body}} ->
        Poison.decode(body)
      other ->
        other
    end
  end

  @doc """
  Deletes a document attachment.

  A rev number is required if only the document ID is given instead of the
  document itself.
  """
  @spec delete_attachment(db :: ICouch.DB.t, doc :: String.t | map, filename :: String.t, options :: [delete_attachment_option]) :: {:ok, map} | {:error, term}
  def delete_attachment(db, doc, filename, options \\ []) do
    {doc_id, options} = case doc do
      %{"_id" => doc_id, "_rev" => doc_rev} ->
        {doc_id, Keyword.put(options, :rev, doc_rev)}
      doc_id when is_binary(doc_id) ->
        {doc_id, options}
    end
    ICouch.DB.send_req(db, {"#{doc_id}/#{URI.encode(filename)}", options}, :delete)
  end

  @doc """
  Cancels a stream operation.
  """
  @spec stream_cancel(ref) :: :ok
  def stream_cancel(ref),
    do: ICouch.StreamTransformer.cancel(ref)

  @doc """
  Builds a full document from the referenced streaming source.
  """
  @spec read_doc_from_stream(ref, timeout :: integer) :: {:ok, Document.t} | {:error, term}
  def read_doc_from_stream(ref, timeout \\ 30_000),
    do: ICouch.StreamTransformer.collect_document(ref, timeout)

  @doc """
  Reads all attachment data from the referenced streaming source.
  """
  @spec read_attachment_data_from_stream(ref, timeout :: integer) :: {:ok, binary} | {:error, term}
  def read_attachment_data_from_stream(ref, timeout),
    do: ICouch.StreamTransformer.collect_attachment(ref, timeout)

  @doc """
  Utility function which calculates the size of the corresponding JSON data of
  the given parameter.

  Since UTF-8 strings are to be used everywhere, this should be accurate.
  """
  def json_byte_size(s) when is_binary(s), do: byte_size(s) + 2
  def json_byte_size(i) when is_integer(i), do: byte_size(Integer.to_string(i))
  def json_byte_size(f) when is_float(f), do: byte_size(Float.to_string(f))
  def json_byte_size(true), do: 4
  def json_byte_size(false), do: 5
  def json_byte_size(:null), do: 4
  def json_byte_size(nil), do: 4
  def json_byte_size(elements) when is_map(elements),
    do: Enum.reduce(elements, 2, fn({k, v}, acc) -> acc + json_byte_size(k) + json_byte_size(v) + 2 end)
  def json_byte_size(elements) when is_list(elements),
    do: Enum.reduce(elements, 2, fn(e, acc) -> acc + json_byte_size(e) + 1 end)

  # -- Private --

  defp setup_stream_translator({:ibrowse_req_id, req_id}, tr_pid) do
    ICouch.StreamTransformer.set_req_id(tr_pid, req_id)
    {status, _} = ICouch.StreamTransformer.get_headers(tr_pid)
    case parse_status_code(status) do
      :ok ->
        {:ok, tr_pid}
      other ->
        ICouch.StreamTransformer.cancel(tr_pid)
        other
    end
  end
  defp setup_stream_translator(other, tr_pid) do
    ICouch.StreamTransformer.cancel(tr_pid)
    other
  end
end
