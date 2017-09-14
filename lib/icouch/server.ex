
# Created by Patrick Schneider on 04.06.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule ICouch.Server do
  @moduledoc """
  Holds information about CouchDB server connections and encapsulates API calls.
  """

  require Logger

  @typedoc """
  An endpoint can be given as plain path, as 2-tuple of a path and (query)
  options or as `URI.t`. The path cannot contain a query string; if you need to
  specify a custom query string, convert the path including the query string
  with `URI.parse/1`.
  """
  @type endpoint ::
    String.t | URI.t | {String.t | URI.t, options :: Keyword.t | %{optional(atom) => term}}

  @type method ::
    :get | :post | :head | :options | :put | :patch | :delete | :trace |
    :mkcol | :propfind | :proppatch | :lock | :unlock | :move | :copy

  @type body ::
    nil | binary |
    (() -> {:ok, binary} | :eof) |
    (state :: term -> {:ok, binary, new_state :: term} | :eof)

  @type option ::
    {:max_sessions, integer} | {:max_pipeline_size, integer} |
    {:ssl_options, [any]} | {:pool_name, atom} |{:proxy_host, String.t} |
    {:proxy_port, integer} | {:proxy_user, String.t} |
    {:proxy_password, String.t} |
    {:basic_auth, {username :: String.t, password :: String.t}} |
    {:cookie, String.t} | {:http_vsn, {major :: integer, minor :: integer}} |
    {:host_header, String.t} | {:timeout, integer} |
    {:inactivity_timeout, integer} | {:connect_timeout, integer} |
    {:max_attempts, integer} | {:socket_options, [any]} |
    {:worker_process_options, [any]} | {:direct_conn_pid, pid}

  defstruct [:uri, :direct, :timeout, :ib_options]

  @type t :: %__MODULE__{
    uri: URI.t,
    direct: nil,
    timeout: nil,
    ib_options: []
  }

  @doc """
  See `ICouch.server_connection/2`.
  """
  @spec new(uri :: String.t | URI.t, options :: [option]) :: t
  def new(uri, options) when is_binary(uri),
    do: new(URI.parse(uri), options)
  def new(%URI{} = uri, options) do
    {uri, uri_options} = parse_uri(uri)
    {direct, timeout, options} = parse_options(uri_options ++ options)
    %ICouch.Server{uri: uri, direct: direct, timeout: timeout, ib_options: options}
  end

  @doc """
  Invokes an arbitrary CouchDB API call which may or may not be implementation
  specific.

  Many functions of the main module make use of this function. It is exposed
  publicly so you can call arbitrary API functions yourself.

  Note that there is transparent JSON encoding and decoding for the requests and
  the response headers are discarded; if you need raw data or the headers, use
  `send_raw_req/6`. Also checks are made if the request method actually allows
  a request body and discards it if needed.
  """
  @spec send_req(server :: t, endpoint, method, body) ::
    {:ok, body :: term} | {:error, ICouch.RequestError.well_known_error | term}
  def send_req(%__MODULE__{} = server, endpoint, method \\ :get, body \\ nil) do
    has_body = body != nil && method_allows_body(method)
    headers = [{"Accept", "application/json"}] ++ (if has_body, do: [{"Content-Type", "application/json"}], else: [])
    case send_raw_req(server, endpoint, method, (if has_body, do: Poison.encode!(body)), headers) do
      {:ok, {_, response_body}} ->
        Poison.decode(response_body)
      other ->
        other
    end
  end

  @doc """
  Invokes an arbitrary CouchDB API call which may or may not be implementation
  specific.

  This expects the request body to be sent as binary (or function yielding
  binary chunks) and returns the raw response headers and the body as binary.
  There are no checks made if the request method actually allows a request body.

  To use transparent JSON encoding and decoding for the requests use `send_req/4`.
  """
  @spec send_raw_req(server :: t, endpoint, method, body, headers :: [{binary, binary}], ib_options :: Keyword.t) ::
    {:ok, {response_headers :: [{binary, binary}], response_body :: binary}} | {:ibrowse_req_id, id :: term} | {:error, ICouch.RequestError.well_known_error | term}
  def send_raw_req(%__MODULE__{uri: uri, direct: conn_pid, timeout: timeout, ib_options: s_ib_options}, endpoint, method \\ :get, body \\ nil, headers \\ [], ib_options \\ []) do
    endpoint = endpoint_with_options(endpoint)
    url = uri |> URI.merge(endpoint) |> URI.to_string |> String.to_charlist
    Logger.debug "ICouch request: [#{method}] #{url}"
    ib_options = Keyword.new(s_ib_options ++ [response_format: :binary] ++ ib_options)
    cond do
      conn_pid == nil and timeout == nil ->
        :ibrowse.send_req(url, headers, method, body || [], ib_options)
      timeout == nil ->
        :ibrowse.send_req_direct(conn_pid, url, headers, method, body || [], ib_options)
      conn_pid == nil ->
        :ibrowse.send_req(url, headers, method, body || [], ib_options, timeout)
      true ->
        :ibrowse.send_req_direct(conn_pid, url, headers, method, body || [], ib_options, timeout)
    end
    |>
    case do
      {:ok, status, response_headers, response_body} ->
        case ICouch.RequestError.parse_status_code(status) do
          :ok ->
            {:ok, {response_headers, response_body}}
          other ->
            other
        end
      other ->
        other
    end
  end

  @doc """
  Encodes CouchDB style query options together with an endpoint and returns the
  resulting relative URI.

  This function is applied to the `entrypoint` parameter of `send_raw_req/6` and
  indirectly to `send_req/4`.

  To be specific, the following option values are converted to a plain string:  
  `rev`, `filter`, `view`, `since`, `startkey_docid`, `endkey_docid`

  These options are deleted:  
  `multipart`, `stream_to`

  The `batch` option is either converted to `:ok` on true, or removed on false.

  Options with values that are atoms are also converted to a plain string.

  Options with nil values are removed.

  Options given through `query_params` are also kept as-is but cannot override
  an option given normally.

  Any other option value is converted to its JSON representation.
  """
  def endpoint_with_options(endpoint, options \\ [])
    
  def endpoint_with_options({endpoint, options}, _),
    do: endpoint_with_options(endpoint, options)
  def endpoint_with_options(endpoint, options) when is_binary(endpoint),
    do: endpoint_with_options(URI.parse(endpoint), options)
  def endpoint_with_options(%URI{} = endpoint, options) do
    options
      |> Enum.reduce([], &parse_endpoint_options/2)
      |> Enum.reverse()
      |> case do
        [] -> %{endpoint | query: nil}
        query -> %{endpoint | query: URI.encode_query(query)}
      end
  end

  # -- Private --

  defp parse_uri(%URI{userinfo: nil} = uri),
    do: {%{uri | fragment: nil, query: nil}, []}
  defp parse_uri(%URI{host: host, userinfo: userinfo} = uri) do
    [username | password] = String.split(userinfo, ":")
    {%{uri | authority: host, userinfo: nil, fragment: nil, query: nil}, [basic_auth: {username, Enum.join(password, ":")}]}
  end

  defp parse_options(options) do
    Keyword.new(options) |> Enum.reduce({nil, nil, []}, fn
      {:basic_auth, {username, password}}, {d, t, o} ->
        {d, t, [{:basic_auth, {String.to_charlist(username), String.to_charlist(password)}} | o]}
      {:direct_conn_pid, d}, {_, t, o} ->
        {d, t, o}
      {:timeout, t}, {d, _, o} ->
        {d, t, o}
      {key, value}, {d, t, o} when key in [:max_sessions, :max_pipeline_size,
          :ssl_options, :pool_name, :proxy_port, :http_vsn, :inactivity_timeout,
          :connect_timeout, :max_attempts, :socket_options,
          :worker_process_options] ->
        {d, t, [{key, value} | o]}
      {key, value}, {d, t, o} when key in [:proxy_host, :proxy_password, :cookie, :host_header] ->
        {d, t, [{key, String.to_charlist(value)} | o]}
      _, acc ->
        acc
    end)
  end

  defp method_allows_body(method) when method in [:post, :put, :patch, :mkcol, :proppatch],
    do: true
  defp method_allows_body(_),
    do: false

  defp parse_endpoint_options({_, nil}, acc),
    do: acc
  defp parse_endpoint_options({:batch, true}, acc),
    do: [{:batch, :ok} | acc]
  defp parse_endpoint_options({:query_params, params}, acc),
    do: Enum.reduce(params, acc, fn {k, v}, acc -> Keyword.put_new(acc, k, v) end)
  defp parse_endpoint_options({key, _}, acc) when key in [:batch, :multipart, :stream_to],
    do: acc
  defp parse_endpoint_options({key, value} = pair, acc) when key in [:rev, :filter, :view, :since, :startkey_docid, :endkey_docid] or is_atom(value),
    do: [pair | acc]
  defp parse_endpoint_options({key, value}, acc),
    do: [{key, Poison.encode!(value)} | acc]
end
