
# Created by Patrick Schneider on 05.06.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule ICouch.DB do
  @moduledoc """
  Holds information about CouchDB databases.
  """

  defstruct [:server, :name]

  @type t :: %__MODULE__{
    server: ICouch.Server.t,
    name: String.t
  }

  @doc """
  Initialize a DB struct.
  """
  @spec new(server :: ICouch.Server.t, name :: String.t) :: t
  def new(%ICouch.Server{} = server, name) when is_binary(name),
    do: %__MODULE__{server: server, name: name}

  @doc """
  Invokes an arbitrary CouchDB API call on a database which may or may not be
  implementation specific.

  See `ICouch.Server.send_req/4`.
  """
  @spec send_req(db :: t, endpoint :: ICouch.Server.endpoint, method :: ICouch.Server.method, body_term :: term) ::
    {:ok, body :: term} | {:error, ICouch.RequestError.well_known_error | term}
  def send_req(%__MODULE__{server: server} = db, endpoint, method \\ :get, body_term \\ nil) do
    ICouch.Server.send_req(server, server_endpoint(db, endpoint), method, body_term)
  end

  @doc """
  Invokes an arbitrary CouchDB API call on a database which may or may not be
  implementation specific.

  See `ICouch.Server.send_raw_req/6`.
  """
  @spec send_raw_req(db :: t, endpoint :: ICouch.Server.endpoint, method :: ICouch.Server.method, body :: term, headers :: [{binary, binary}], ib_options :: Keyword.t) ::
    {:ok, {response_headers :: [{binary, binary}], body :: binary}} | {:ibrowse_req_id, id :: term} | {:error, ICouch.RequestError.well_known_error | term}
  def send_raw_req(%__MODULE__{server: server} = db, endpoint, method \\ :get, body \\ nil, headers \\ [], ib_options \\ []) do
    ICouch.Server.send_raw_req(server, server_endpoint(db, endpoint), method, body, headers, ib_options)
  end

  @doc """
  Internal function that checks if a database exists.
  """
  @spec exists(db :: t) :: {:ok, t} | {:error, ICouch.RequestError.well_known_error | term}
  def exists(db) do
    case send_raw_req(db, "", :head) do
      {:ok, _} -> {:ok, db}
      other -> other
    end
  end

  @doc """
  Internal function to build a server endpoint.

  Basically prepends the database name to the given URL path segment.
  """
  @spec server_endpoint(db :: t, endpoint :: ICouch.Server.endpoint) :: URI.t
  def server_endpoint(%__MODULE__{name: db_name}, {path, options}) when is_binary(path),
    do: {"#{db_name}/#{path}", options}
  def server_endpoint(%__MODULE__{name: db_name}, %URI{path: path} = endpoint),
    do: %{endpoint | path: "#{db_name}/#{path}"}
  def server_endpoint(%__MODULE__{name: db_name}, endpoint) when is_binary(endpoint),
    do: "#{db_name}/#{endpoint}"
end
