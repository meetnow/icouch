
# Created by Patrick Schneider on 03.06.2017.
# Copyright (c) 2017,2018 MeetNow! GmbH

defmodule ICouch do
  @moduledoc """
  Main module of ICouch.

  Note: Multipart transmission is the default; you can disable it by setting
  adding option `multipart: false`. For saving, it is also possible to use a
  specific multipart boundary instead of a random one by setting the option to
  a string instead of boolean.
  """

  use ICouch.RequestError

  alias ICouch.Document

  @type create_db_option ::
    {:q, integer}

  @type open_doc_option ::
    {:attachments, boolean} | {:att_encoding_info, boolean} |
    {:atts_since, [String.t]} | {:conflicts, boolean} |
    {:deleted_conflicts, boolean} | {:latest, boolean} | {:local_seq, boolean} |
    {:meta, boolean} | {:open_revs, [String.t]} | {:rev, String.t} |
    {:revs, boolean} | {:revs_info, boolean} | {:multipart, boolean}

  @type open_docs_option ::
    {:revs, boolean}

  @type save_doc_option ::
    {:new_edits, boolean} | {:batch, boolean} | {:multipart, boolean | String.t}

  @type copy_delete_doc_option ::
    {:rev, String.t} | {:batch, boolean}

  @type fetch_attachment_option ::
    {:rev, String.t}

  @type put_attachment_option ::
    {:rev, String.t} | {:content_type, String.t} | {:content_length, integer}

  @type delete_attachment_option ::
    {:rev, String.t}

  @type open_view_option ::
    {:conflicts, boolean} | {:descending, boolean} | {:endkey, String.t} |
    {:endkey_docid, String.t} | {:group, boolean} | {:group_level, integer} |
    {:include_docs, boolean} | {:attachments, boolean} |
    {:att_encoding_info, boolean} | {:inclusive_end, boolean} |
    {:key, String.t} | {:keys, [String.t]} | {:limit, integer} |
    {:reduce, boolean} | {:skip, integer} | {:stale, :ok | :update_after} |
    {:startkey, String.t} | {:startkey_docid, String.t} | {:update_seq, boolean}

  @type open_changes_option ::
    {:doc_ids, [String.t]} | {:conflicts, boolean} | {:descending, boolean} |
    {:filter, String.t} | {:include_docs, boolean} | {:attachments, boolean} |
    {:att_encoding_info, boolean} | {:limit, integer} |
    {:since, String.t | integer} | {:style, :main_only | :all_docs} |
    {:view, String.t} | {:query_params, map | Keyword.t}

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
  def server_info(server),
    do: ICouch.Server.send_req(server, "")

  @doc """
  Returns the server's membership.

  CouchDB >= 2.0 only.
  """
  @spec server_membership(server :: ICouch.Server.t) :: {:ok, map} | {:error, term}
  def server_membership(server),
    do: ICouch.Server.send_req(server, "_membership")

  @doc """
  Returns the entire CouchDB server configuration.

  CouchDB < 2.0 only.
  """
  @spec get_config(server :: ICouch.Server.t) :: {:ok, map} | {:error, term}
  def get_config(server),
    do: ICouch.Server.send_req(server, "_config")

  @doc """
  Gets the configuration structure for a single section.

  CouchDB < 2.0 only.
  """
  @spec get_config(server :: ICouch.Server.t, section :: String.t) :: {:ok, map} | {:error, term}
  def get_config(server, section),
    do: ICouch.Server.send_req(server, "_config/#{URI.encode(section)}")

  @doc """
  Gets a single configuration value from within a specific configuration section.

  CouchDB < 2.0 only.
  """
  @spec get_config(server :: ICouch.Server.t, section :: String.t, key :: String.t) :: {:ok, map} | {:error, term}
  def get_config(server, section, key),
    do: ICouch.Server.send_req(server, "_config/#{URI.encode(section)}/#{URI.encode(key)}")

  @doc """
  Updates a configuration value. The new value should be in the correct
  JSON-serializable format. In response CouchDB sends old value for target
  section key.

  CouchDB < 2.0 only.
  """
  @spec set_config(server :: ICouch.Server.t, section :: String.t, key :: String.t, value :: term) :: {:ok, term} | {:error, term}
  def set_config(server, section, key, value),
    do: ICouch.Server.send_req(server, "_config/#{URI.encode(section)}/#{URI.encode(key)}", :put, value)

  @doc """
  Deletes a configuration value. The returned JSON will be the value of the
  configuration parameter before it was deleted.

  CouchDB < 2.0 only.
  """
  @spec delete_config(server :: ICouch.Server.t, section :: String.t, key :: String.t) :: {:ok, term} | {:error, term}
  def delete_config(server, section, key),
    do: ICouch.Server.send_req(server, "_config/#{URI.encode(section)}/#{URI.encode(key)}", :delete)

  @doc """
  Returns the entire CouchDB node configuration.

  CouchDB >= 2.0 only.
  """
  @spec get_node_config(server :: ICouch.Server.t, node_name :: String.t) :: {:ok, map} | {:error, term}
  def get_node_config(server, node_name),
    do: ICouch.Server.send_req(server, "_node/#{URI.encode(node_name)}/_config")

  @doc """
  Gets the configuration structure for a single section of a node.

  CouchDB >= 2.0 only.
  """
  @spec get_node_config(server :: ICouch.Server.t, node_name :: String.t, section :: String.t) :: {:ok, map} | {:error, term}
  def get_node_config(server, node_name, section),
    do: ICouch.Server.send_req(server, "_node/#{URI.encode(node_name)}/_config/#{URI.encode(section)}")

  @doc """
  Gets a single configuration value from within a specific configuration section
  of a node.

  CouchDB >= 2.0 only.
  """
  @spec get_node_config(server :: ICouch.Server.t, node_name :: String.t, section :: String.t, key :: String.t) :: {:ok, map} | {:error, term}
  def get_node_config(server, node_name, section, key),
    do: ICouch.Server.send_req(server, "_node/#{URI.encode(node_name)}/_config/#{URI.encode(section)}/#{URI.encode(key)}")

  @doc """
  Updates a configuration value on a node. The new value should be in the
  correct JSON-serializable format. In response CouchDB sends old value for target
  section key.

  CouchDB >= 2.0 only.
  """
  @spec set_node_config(server :: ICouch.Server.t, node_name :: String.t, section :: String.t, key :: String.t, value :: term) :: {:ok, term} | {:error, term}
  def set_node_config(server, node_name, section, key, value),
    do: ICouch.Server.send_req(server, "_node/#{URI.encode(node_name)}/_config/#{URI.encode(section)}/#{URI.encode(key)}", :put, value)

  @doc """
  Deletes a configuration value from a node. The returned JSON will be the value
  of the configuration parameter before it was deleted.

  CouchDB >= 2.0 only.
  """
  @spec delete_node_config(server :: ICouch.Server.t, node_name :: String.t, section :: String.t, key :: String.t) :: {:ok, term} | {:error, term}
  def delete_node_config(server, node_name, section, key),
    do: ICouch.Server.send_req(server, "_node/#{URI.encode(node_name)}/_config/#{URI.encode(section)}/#{URI.encode(key)}", :delete)

  @doc """
  Tests if the server is on an admin party.

  This is achieved by temporarily stripping credentials from the server struct
  and trying to call `server_membership/1` (followed by a call to
  `get_config(server, "couchdb", "database_dir")` if the server does not have
  that route).
  """
  @spec has_admin_party?(server :: ICouch.Server.t) :: boolean
  def has_admin_party?(server) do
    server = ICouch.Server.delete_credentials(server)
    case server_membership(server) do
      {:error, :unauthorized} ->
        false
      {:ok, _} ->
        true
      _ ->
        case get_config(server, "couchdb", "database_dir") do
          {:error, :unauthorized} ->
            false
          _ ->
            true
        end
    end
  end

  @doc """
  Creates a new local admin user with the given password.

  This works for any CouchDB version by checking if the "_membership" route
  exists and then using `set_config/4` or `set_node_config/5` respectively.
  """
  @spec create_admin(server :: ICouch.Server.t, username :: String.t, password :: String.t) :: {:ok, term} | {:error, term}
  def create_admin(server, username, password) do
    case server_membership(server) do
      {:ok, _} ->
        set_node_config(server, "_local", "admins", username, password)
      {:error, :unauthorized} = err ->
        err
      _ ->
        set_config(server, "admins", username, password)
    end
  end

  @doc """
  Returns a list of all the databases in the CouchDB instance.
  """
  @spec all_dbs(server :: ICouch.Server.t) :: {:ok, [String.t]} | {:error, term}
  def all_dbs(server),
    do: ICouch.Server.send_req(server, "_all_dbs")

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
  def get_uuid!(server),
    do: req_result_or_raise! get_uuid(server)

  @doc """
  Requests one or more UUIDs from the CouchDB instance.

  Note that this is not strictly UUID compliant; it will in fact return a list
  of 32 character hexadecimal strings.
  """
  @spec get_uuids(server :: ICouch.Server.t, count :: integer) :: {:ok, [String.t]} | {:error, term}
  def get_uuids(server, count) do
    case ICouch.Server.send_req(server, {"_uuids", [count: count]}) do
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
  def get_uuids!(server, count),
    do: req_result_or_raise! get_uuids(server, count)

  @doc """
  Opens an existing database and returns a handle on success.

  Does check if the database exists.
  """
  @spec open_db(server :: ICouch.Server.t, db_name :: String.t) :: {:ok, ICouch.DB.t} | {:error, term}
  def open_db(server, db_name),
    do: ICouch.DB.new(server, db_name) |> ICouch.DB.exists()

  @doc """
  Same as `open_db/2` but returns the database directly on success or raises
  an error on failure.
  """
  @spec open_db!(server :: ICouch.Server.t, db_name :: String.t) :: ICouch.DB.t
  def open_db!(server, db_name),
    do: req_result_or_raise! open_db(server, db_name)

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
  @spec create_db(server :: ICouch.Server.t, db_name :: String.t, options :: [create_db_option]) :: {:ok, ICouch.DB.t} | {:error, term}
  def create_db(server, db_name, options \\ []) do
    db = ICouch.DB.new(server, db_name)
    case ICouch.DB.send_raw_req(db, {"", options}, :put) do
      {:ok, _} -> {:ok, db}
      other -> other
    end
  end

  @doc """
  Same as `create_db/3` but returns the database directly on success or raises
  an error on failure.
  """
  @spec create_db!(server :: ICouch.Server.t, db_name :: String.t, options :: [create_db_option]) :: ICouch.DB.t
  def create_db!(server, db_name, options \\ []),
    do: req_result_or_raise! create_db(server, db_name, options)

  @doc """
  Opens or creates a database and returns a handle on success.

  Returns an error if the database could not be created if it was missing.

  Implementation note: this tries to create the database first.
  """
  @spec assert_db(server :: ICouch.Server.t, db_name :: String.t, options :: [create_db_option]) :: {:ok, ICouch.DB.t} | {:error, term}
  def assert_db(server, db_name, options \\ []) do
    case create_db(server, db_name, options) do
      {:ok, _} = created ->
        created
      {:error, :precondition_failed} ->
        {:ok, ICouch.DB.new(server, db_name)}
      other ->
        other
    end
  end

  @doc """
  Same as `assert_db/3` but returns the database directly on success or raises
  an error on failure.
  """
  @spec assert_db!(server :: ICouch.Server.t, db_name :: String.t, options :: [create_db_option]) :: ICouch.DB.t
  def assert_db!(server, db_name, options \\ []),
    do: req_result_or_raise! assert_db(server, db_name, options)

  @doc """
  Gets information about the database.
  """
  @spec db_info(db :: ICouch.DB.t) :: {:ok, map} | {:error, term}
  def db_info(db),
    do: ICouch.DB.send_req(db, "")

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
  Returns the current security object from the specified database.
  """
  @spec get_security(db :: ICouch.DB.t) :: {:ok, map} | {:error, term}
  def get_security(db),
    do: ICouch.DB.send_req(db, "_security")

  @doc """
  Sets the security object for the given database.
  """
  @spec set_security(db :: ICouch.DB.t, obj :: map) :: :ok | {:error, term}
  def set_security(db, obj) when is_map(obj) do
    case ICouch.DB.send_req(db, "_security", :put, obj) do
      {:ok, _} -> :ok
      other -> other
    end
  end

  @doc """
  Gets the current revision limit setting.

  CouchDB >= 2.0 only.
  """
  @spec get_revs_limit(db :: ICouch.DB.t) :: {:ok, integer} | {:error, term}
  def get_revs_limit(db),
    do: ICouch.DB.send_req(db, "_revs_limit")

  @doc """
  Gets the current revision limit setting.

  CouchDB >= 2.0 only.
  """
  @spec set_revs_limit(db :: ICouch.DB.t, limit :: integer) :: :ok | {:error, term}
  def set_revs_limit(db, limit) do
    case ICouch.DB.send_req(db, "_revs_limit", :put, limit) do
      {:ok, _} -> :ok
      other -> other
    end
  end

  @doc """
  Tests if a document exists.
  """
  @spec doc_exists?(db :: ICouch.DB.t, doc_id :: String.t, options :: [open_doc_option]) :: boolean
  def doc_exists?(db, doc_id, options \\ []) do
    case ICouch.DB.send_raw_req(db, {doc_id, options}, :head) do
      {:ok, _} -> true
      _ -> false
    end
  end

  @doc """
  Retrieves the last revision of the document with the specified id.
  """
  @spec get_doc_rev(db :: ICouch.DB.t, doc_id :: String.t) :: {:ok, String.t} | {:error, term}
  def get_doc_rev(db, doc_id) do
    case ICouch.DB.send_raw_req(db, doc_id, :head) do
      {:ok, {headers, _}} ->
        case Enum.find_value(headers, fn
              {key, value} when is_list(key) -> :string.to_lower(key) == 'etag' && Poison.decode(value)
              {key, value} when is_binary(key) -> String.downcase(key) == "etag" && Poison.decode(value)
            end) do
          {:ok, _} = result -> result
          _ -> {:error, :invalid_response}
        end
      other ->
        other
    end
  end

  @doc """
  Opens a document in a database.
  """
  @spec open_doc(db :: ICouch.DB.t, doc_id :: String.t, options :: [open_doc_option]) :: {:ok, Document.t} | {:error, term}
  def open_doc(db, doc_id, options \\ []) do
    if options[:stream_to] != nil do
      raise ArgumentError, "stream_to is not allowed anymore, please use the stream_doc function"
    end
    if Keyword.get(options, :multipart, true) and Keyword.get(options, :attachments, false) do
      case ICouch.DB.send_raw_req(db, {doc_id, options}, :get, nil, [{"Accept", "multipart/related, application/json"}, {"Accept-Encoding", "gzip"}]) do
        {:ok, {headers, body}} ->
          case ICouch.Multipart.get_boundary(headers) do
            {:ok, _, boundary} ->
              case ICouch.Multipart.split(body, boundary) do
                {:ok, parts} ->
                  Document.from_multipart(parts)
                _ ->
                  {:error, :invalid_response}
              end
            _ ->
              document_from_body(headers, body)
          end
        other ->
          other
      end
    else
      case ICouch.DB.send_raw_req(db, {doc_id, options}, :get, nil, [{"Accept", "application/json"}, {"Accept-Encoding", "gzip"}]) do
        {:ok, {headers, body}} ->
          document_from_body(headers, body)
        other ->
          other
      end
    end
  end

  @doc """
  Same as `open_doc/3` but returns the document directly on success or raises
  an error on failure.
  """
  @spec open_doc!(db :: ICouch.DB.t, doc_id :: String.t, options :: [open_doc_option]) :: map
  def open_doc!(db, doc_id, options \\ []),
    do: req_result_or_raise! open_doc(db, doc_id, options)

  @doc """
  Opens several documents in a database.

  The parameter can be a list of strings (document IDs) or tuples of two strings
  (document ID + revision) or tuples of three strings (document ID + revision +
  attributes since) or maps (see CouchDB documentation) or any mix of those.

  CouchDB >= 2.0 only.
  You can similar results using the "_all_docs" view on CouchDB < 2.0.
  """
  @spec open_docs(db :: ICouch.DB.t, doc_ids_revs :: [String.t | {String.t, String.t} | {String.t, String.t, String.t} | map], options :: [open_docs_option]) :: {:ok, [Document.t]} | {:error, term}
  def open_docs(db, doc_ids_revs, options \\ []) do
    post_body = Poison.encode!(%{"docs" => Enum.map(doc_ids_revs, fn
      entry when is_map(entry) ->
        entry
      doc_id when is_binary(doc_id) ->
        %{"id" => doc_id}
      {doc_id, doc_rev} when is_binary(doc_id) and is_binary(doc_rev) ->
        %{"id" => doc_id, "rev" => doc_rev}
      {doc_id, doc_rev, atts_since} when is_binary(doc_id) and is_binary(doc_rev) and is_binary(atts_since) ->
        %{"id" => doc_id, "rev" => doc_rev, "atts_since" => atts_since}
    end)})
    case ICouch.DB.send_raw_req(db, {"_bulk_get", options}, :post, post_body, [{"Content-Type", "application/json"}, {"Accept", "application/json"}, {"Accept-Encoding", "gzip"}]) do
      {:ok, {headers, body}} ->
        documents_from_body(headers, body)
      other ->
        other
    end
  end

  @doc """
  Same as `open_docs/3` but returns the documents directly on success or raises
  an error on failure.
  """
  @spec open_docs!(db :: ICouch.DB.t, doc_ids_revs :: [String.t | {String.t, String.t} | {String.t, String.t, String.t} | map], options :: [open_docs_option]) :: [Document.t]
  def open_docs!(db, doc_ids_revs, options \\ []),
    do: req_result_or_raise! open_docs(db, doc_ids_revs, options)

  @doc """
  Start streaming a document in a database to the given process.

  See `ICouch.StreamChunk` and `ICouch.StreamEnd`. The returned value will be
  the stream reference. If attachments are requested, they will be streamed
  separately, even if the server does not support multipart.
  """
  @spec stream_doc(db :: ICouch.DB.t, doc_id :: String.t, stream_to :: pid, options :: [open_doc_option]) :: {:ok, ref} | {:error, term}
  def stream_doc(db, doc_id, stream_to, options \\ []) do
    multipart = Keyword.get(options, :multipart, true) and Keyword.get(options, :attachments, false)
    tr_pid = ICouch.StreamTransformer.spawn(:document, doc_id, stream_to)
    ICouch.DB.send_raw_req(db, {doc_id, options}, :get, nil, [{"Accept", (if multipart, do: "multipart/related, ", else: "") <> "application/json"}], [stream_to: tr_pid])
      |> setup_stream_translator(tr_pid)
  end

  @doc """
  Creates a new document or creates a new revision of an existing document.

  If the document does not have an "_id" property, the function will obtain a
  new UUID via `get_uuid/1`.

  Returns the saved document with updated revision on success. If a map is
  passed instead of a Document struct, it will be converted to a
  `ICouch.Document.t` first.
  """
  @spec save_doc(db :: ICouch.DB.t, doc :: map | Document.t, options :: [save_doc_option]) :: {:ok, Document.t} | {:error, term}
  def save_doc(db, doc, options \\ [])

  def save_doc(db, %Document{id: doc_id, attachment_data: atts} = doc, options) when doc_id != nil do
    multipart = Keyword.get(options, :multipart, true)
    if multipart && map_size(atts) > 0 do
      case Document.to_multipart(doc) do
        {:ok, parts} ->
          boundary = if String.valid?(multipart) and byte_size(multipart) > 0,
            do: multipart,
            else: Base.encode16(:erlang.list_to_binary(Enum.map(1..20, fn _ -> :rand.uniform(256)-1 end)))
          body = ICouch.Multipart.join(parts, boundary)
          case ICouch.DB.send_raw_req(db, {doc_id, options}, :put, body, [{"Content-Type", "multipart/related; boundary=\"#{boundary}\""}]) do
            {:ok, {_, body}} ->
              case Poison.decode(body) do
                {:ok, %{"rev" => new_rev}} ->
                  {:ok, Document.set_rev(doc, new_rev)}
                _ ->
                  {:error, :invalid_response}
              end
            other ->
              other
          end
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
  def save_doc(%ICouch.DB{server: server} = db, %Document{} = doc, options) do
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
  @spec save_doc!(db :: ICouch.DB.t, doc :: map | Document.t, options :: [save_doc_option]) :: map
  def save_doc!(db, doc, options \\ []),
    do: req_result_or_raise! save_doc(db, doc, options)

  @doc """
  Creates and updates multiple documents at the same time within a single
  request.

  It is possible to mix Document structs and plain maps.
  """
  @spec save_docs(db :: ICouch.DB.t, docs :: [map | Document.t], options :: [save_doc_option]) :: {:ok, [map]} | {:error, term}
  def save_docs(db, docs, options \\ []) do
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
  @spec dup_doc(db :: ICouch.DB.t, src_doc :: String.t | Document.t | map, options :: [copy_delete_doc_option]) :: {:ok, map} | {:error, term}
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
  @spec copy_doc(db :: ICouch.DB.t, src_doc :: String.t | Document.t | map, dest_doc_id :: String.t, options :: [copy_delete_doc_option]) :: {:ok, map} | {:error, term}
  def copy_doc(db, src_doc, dest_doc_id, options \\ []) do
    src_doc_id = case src_doc do
      %Document{id: src_doc_id} ->
        src_doc_id
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
  @spec delete_doc(db :: ICouch.DB.t, doc :: String.t | Document.t | map, options :: [copy_delete_doc_option]) :: {:ok, map} | {:error, term}
  def delete_doc(db, doc, options \\ []) do
    {doc_id, options} = case doc do
      %Document{id: doc_id, rev: doc_rev} when doc_id != nil and doc_rev != nil ->
        {doc_id, Keyword.put(options, :rev, doc_rev)}
      %{"_id" => doc_id, "_rev" => doc_rev} ->
        {doc_id, Keyword.put(options, :rev, doc_rev)}
      doc_id when is_binary(doc_id) ->
        {doc_id, options}
    end
    ICouch.DB.send_req(db, {doc_id, options}, :delete)
  end

  @doc """
  Opens a view in a database.

  This will check if the design document exists and return a `ICouch.View.t`
  struct which can be iterated using `Enum` functions. The returned view is
  unfetched.

  The name should be in the form `design_doc_name/view_name` except for
  "_all_docs", "_design_docs" and "_local_docs".

  Note that the "_all_docs", "_design_docs" and "_local_docs" view is not
  checked for existence; the latter two are only available on CouchDB >= 2.0.
  """
  @spec open_view(db :: ICouch.DB.t, name :: String.t, options :: [open_view_option]) :: {:ok, ICouch.View.t} | {:error, term}
  def open_view(%ICouch.DB{} = db, name, options \\ []) do
    case String.split(name, "/", parts: 2) do
      [builtin_view] when builtin_view in ["_all_docs", "_design_docs", "_local_docs"] ->
        {:ok, %ICouch.View{db: db, name: builtin_view, params: Map.new(options)}}
      [_] ->
        raise ArgumentError, message: "invalid view name"
      [ddoc, view_name] ->
        if doc_exists?(db, "_design/#{ddoc}") do
          {:ok, %ICouch.View{db: db, ddoc: ddoc, name: view_name, params: Map.new(options)}}
        else
          {:error, :not_found}
        end
    end
  end

  @doc """
  Same as `open_view/3` but returns the view struct directly on success or
  raises an error on failure.
  """
  @spec open_view!(db :: ICouch.DB.t, name :: String.t, options :: [open_view_option]) :: ICouch.View.t
  def open_view!(db, doc, options \\ []),
    do: req_result_or_raise! open_view(db, doc, options)

  @doc """
  Opens a changes feed in a database.

  This always succeeds and returns an unfetched `ICouch.Changes.t` struct.

  Note that when setting the `doc_ids` option, any given `filter` option will be
  ignored while fetching changes.

  See also: `ChangesFollower`
  """
  @spec open_changes(db :: ICouch.DB.t, options :: [open_changes_option]) :: ICouch.Changes.t
  def open_changes(%ICouch.DB{} = db, options \\ []),
    do: ICouch.Changes.set_options(%ICouch.Changes{db: db, params: %{}}, options)

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
  """
  @spec fetch_attachment(db :: ICouch.DB.t, doc :: String.t | map, filename :: String.t, options :: [fetch_attachment_option]) :: {:ok, binary} | {:error, term}
  def fetch_attachment(db, doc, filename, options \\ []) do
    if options[:stream_to] != nil do
      raise ArgumentError, "stream_to is not allowed anymore, please use the fetch_attachment function"
    end
    doc_id = case doc do
      %{"_id" => doc_id} ->
        doc_id
      doc_id when is_binary(doc_id) ->
        doc_id
    end
    case ICouch.DB.send_raw_req(db, {"#{doc_id}/#{URI.encode(filename)}", options}, :get, nil, [{"Accept-Encoding", "gzip"}]) do
      {:ok, {headers, body}} ->
        if ICouch.Server.has_gzip_encoding?(headers) do
          try do
            {:ok, :zlib.gunzip(body)}
          rescue _ ->
            {:error, :invalid_response}
          end
        else
          {:ok, body}
        end
      other ->
        other
    end
  end

  @doc """
  Same as `fetch_attachment/4` but returns the data directly on success or
  raises an error on failure.
  """
  @spec fetch_attachment!(db :: ICouch.DB.t, doc :: String.t | map, filename :: String.t, options :: [fetch_attachment_option]) :: binary | ref
  def fetch_attachment!(db, doc, filename, options \\ []),
    do: req_result_or_raise! fetch_attachment(db, doc, filename, options)

  @doc """
  Start streaming a document attachment to the given process.

  See `ICouch.StreamChunk` and `ICouch.StreamEnd`. The returned value will be
  the stream reference.
  """
  @spec stream_attachment(db :: ICouch.DB.t, doc :: String.t | map, filename :: String.t, stream_to :: pid, options :: [fetch_attachment_option]) :: {:ok, ref} | {:error, term}
  def stream_attachment(db, doc, filename, stream_to, options \\ []) do
    doc_id = case doc do
      %{"_id" => doc_id} ->
        doc_id
      doc_id when is_binary(doc_id) ->
        doc_id
    end
    tr_pid = ICouch.StreamTransformer.spawn(:attachment, {doc_id, filename}, stream_to)
    ICouch.DB.send_raw_req(db, {"#{doc_id}/#{URI.encode(filename)}", options}, :get, nil, [{"Accept-Encoding", "identity"}], [stream_to: tr_pid])
      |> setup_stream_translator(tr_pid)
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
  def json_byte_size([]), do: 2
  def json_byte_size(elements) when is_list(elements),
    do: Enum.reduce(elements, 1, &(&2 + json_byte_size(&1) + 1))
  def json_byte_size(elements) when is_map(elements) do
    if map_size(elements) == 0, do: 2,
      else: Enum.reduce(elements, 1, fn {k, v}, acc -> acc + json_byte_size(k) + json_byte_size(v) + 2 end)
  end

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

  defp document_from_body(headers, body) do
    if ICouch.Server.has_gzip_encoding?(headers) do
      try do
        {:ok, :zlib.gunzip(body)}
      rescue _ ->
        {:error, :invalid_response}
      end
    else
      {:ok, body}
    end
    |>
    case do
      {:ok, dec_body} ->
        Document.from_api(dec_body)
      other ->
        other
    end
  end

  defp documents_from_body(headers, body) do
    if ICouch.Server.has_gzip_encoding?(headers) do
      try do
        {:ok, :zlib.gunzip(body)}
      rescue _ ->
        {:error, :invalid_response}
      end
    else
      {:ok, body}
    end
    |>
    case do
      {:ok, dec_body} ->
        Poison.decode(dec_body)
      other ->
        other
    end
    |>
    case do
      {:ok, %{"results" => results}} when is_list(results) ->
        {:ok, List.flatten(for %{"docs" => docs} <- results do
          for %{"ok" => doc} <- docs do Document.from_api!(doc) end
        end)}
      {:ok, _} ->
        {:error, :invalid_response}
      other ->
        other
    end
  end
end
