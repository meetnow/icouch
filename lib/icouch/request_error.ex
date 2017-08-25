
# Created by Patrick Schneider on 05.06.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule ICouch.RequestError do
  @moduledoc """
  Wraps REST and connection errors from CouchDB.
  """

  @type well_known_error ::
    :not_modified | :bad_request | :unauthorized | :forbidden | :not_found |
    :method_not_allowed | :conflict | :precondition_failed |
    :unsupported_media_type | :expectation_failed | :internal_server_error |
    {:status_code, integer} | {:conn_failed, term} | :invalid_response

  defexception reason: :invalid_response, message: "Invalid Response"

  @doc """
  Parses a status code from ibrowse to a well-known error.
  """
  @spec parse_status_code(list | :timeout) :: :ok | {:error, well_known_error}
  def parse_status_code(:timeout), do: {:error, {:conn_failed, :timeout}}
  def parse_status_code('304'), do: {:error, :not_modified}
  def parse_status_code('400'), do: {:error, :bad_request}
  def parse_status_code('401'), do: {:error, :unauthorized}
  def parse_status_code('403'), do: {:error, :forbidden}
  def parse_status_code('404'), do: {:error, :not_found}
  def parse_status_code('405'), do: {:error, :method_not_allowed}
  def parse_status_code('409'), do: {:error, :conflict}
  def parse_status_code('412'), do: {:error, :precondition_failed}
  def parse_status_code('415'), do: {:error, :unsupported_media_type}
  def parse_status_code('417'), do: {:error, :expectation_failed}
  def parse_status_code('500'), do: {:error, :internal_server_error}
  def parse_status_code(status) when status in ['200', '201', '202'], do: :ok
  def parse_status_code(status), do: {:error, {:status_code, List.to_integer(status)}}

  @doc """
  Translates a well-known error to an message to be used in the exception.
  """
  @spec message_for_reason(error :: well_known_error | term) :: String.t
  def message_for_reason(:invalid_response),
    do: "Invalid Response"
  def message_for_reason(:not_modified),
    do: "Not Modified"
  def message_for_reason(:bad_request),
    do: "Bad Request"
  def message_for_reason(:unauthorized),
    do: "Unauthorized"
  def message_for_reason(:forbidden),
    do: "Forbidden"
  def message_for_reason(:not_found),
    do: "Not Found"
  def message_for_reason(:method_not_allowed),
    do: "Method Not Allowed"
  def message_for_reason(:conflict),
    do: "Conflict"
  def message_for_reason(:precondition_failed),
    do: "Precondition Failed"
  def message_for_reason(:unsupported_media_type),
    do: "Unsupported Media Type"
  def message_for_reason(:expectation_failed),
    do: "Expectation Failed"
  def message_for_reason(:internal_server_error),
    do: "Internal Server Error"
  def message_for_reason({:status_code, status_code}),
    do: "Unexpected Status Code: #{status_code}"
  def message_for_reason({:conn_failed, _}),
    do: "Connection Failed"
  def message_for_reason(_),
    do: "Unknown Error"

  defmacro __using__(_) do
    quote do
      import ICouch.RequestError
    end
  end

  defmacro req_result_or_raise!(call) do
    quote do
      case unquote(call) do
        {:ok, result} -> result
        {:error, reason} -> raise ICouch.RequestError, reason: reason, message: ICouch.RequestError.message_for_reason(reason)
      end
    end
  end
end
