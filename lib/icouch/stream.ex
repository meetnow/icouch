
# Created by Patrick Schneider on 18.08.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule ICouch.StreamChunk do
  @moduledoc """
  Struct module for stream chunks.

  The `name` field will hold the document ID on document streaming, both 
  document ID and attachment name on attachment streaming and nil on changes
  streaming.
  """

  defstruct [:ref, :type, :name, :data]

  @type t :: %__MODULE__{
    ref: pid | reference,
    type: :document | :attachment,
    name: String.t | {String.t, String.t} | nil,
    data: binary | map | ICouch.Document.t | nil
  }

  def for_document(ref, doc_id, data),
    do: %__MODULE__{ref: ref, type: :document, name: doc_id, data: data}
  def for_attachment(ref, doc_id, filename, data),
    do: %__MODULE__{ref: ref, type: :attachment, name: {doc_id, filename}, data: data}
end

defmodule ICouch.StreamEnd do
  @moduledoc """
  Struct module for stream ends.

  The document stream ends when all attachments have been streamed.
  """

  defstruct [:ref, :type, :name]

  @type t :: %__MODULE__{
    ref: pid | reference,
    type: :document | :attachment,
    name: String.t | {String.t, String.t} | nil
  }

  def for_document(ref, doc_id),
    do: %__MODULE__{ref: ref, type: :document, name: doc_id}
  def for_attachment(ref, doc_id, filename),
    do: %__MODULE__{ref: ref, type: :attachment, name: {doc_id, filename}}
end

# Internal module for ibrowse stream transformations
defmodule ICouch.StreamTransformer do
  @moduledoc false

  alias ICouch.{StreamChunk, StreamEnd}

  @doc false
  def spawn(:document, doc_id, stream_to),
    do: Kernel.spawn(__MODULE__, :transform_document, [self(), doc_id, stream_to, nil, "", nil])
  def spawn(:attachment, {doc_id, filename}, stream_to),
    do: Kernel.spawn(__MODULE__, :transform_attachment, [self(), doc_id, filename, stream_to, nil])

  @doc false
  def set_req_id(pid, req_id),
    do: send(pid, {:set_req_id, req_id})

  @doc false
  def get_headers(pid) do
    receive do
      {__MODULE__, ^pid, status_code, headers} -> {status_code, headers}
    end
  end

  @doc false
  def cancel(pid) do
    send(pid, :cancel)
    :ok
  end

  @doc false
  def transform_document(origin, doc_id, stream_to, multipart, buffer, req_id) do
    receive do
      {:set_req_id, new_req_id} ->
        transform_document(origin, doc_id, stream_to, multipart, buffer, new_req_id)
      :cancel ->
        if req_id != nil, do: :ibrowse.stream_close(req_id)
        :cancel
      {:ibrowse_async_headers, ^req_id, status_code, headers} ->
        send(origin, {__MODULE__, self(), status_code, headers})
        multipart = case ICouch.Multipart.get_boundary(headers) do
          {:ok, _, boundary} ->
            {:init, boundary}
          _ ->
            nil
        end
        transform_document(origin, doc_id, stream_to, multipart, buffer, req_id)
      {:ibrowse_async_raw_req, ^req_id} ->
        transform_document(origin, doc_id, stream_to, multipart, buffer, req_id)
      {:ibrowse_async_response, ^req_id, data} when multipart in [nil, :end] ->
        transform_document(origin, doc_id, stream_to, multipart, buffer <> data, req_id)
      {:ibrowse_async_response, ^req_id, data} ->
        transform_document_multipart(origin, doc_id, stream_to, multipart, buffer <> data, req_id)
      {:ibrowse_async_response_end, ^req_id} ->
        if multipart == nil, do: decode_send_doc(doc_id, stream_to, buffer)
        send(stream_to, StreamEnd.for_document(self(), doc_id))
        :ok
      {:ibrowse_async_response_timeout, ^req_id} ->
        send(origin, {__MODULE__, self(), :timeout, []})
        :ok
    end
  end

  @doc false
  def transform_attachment(origin, doc_id, filename, stream_to, req_id) do
    receive do
      {:set_id, new_id} ->
        transform_attachment(origin, doc_id, filename, stream_to, new_id)
      :cancel ->
        if req_id != nil, do: :ibrowse.stream_close(req_id)
        :cancel
      {:ibrowse_async_headers, ^req_id, status_code, headers} ->
        send(origin, {__MODULE__, self(), status_code, headers})
        transform_attachment(origin, doc_id, filename, stream_to, req_id)
      {:ibrowse_async_raw_req, ^req_id} ->
        transform_attachment(origin, doc_id, filename, stream_to, req_id)
      {:ibrowse_async_response, ^req_id, data} ->
        send(stream_to, StreamChunk.for_attachment(self(), doc_id, filename, data))
        transform_attachment(origin, doc_id, filename, stream_to, req_id)
      {:ibrowse_async_response_end, ^req_id} ->
        send(stream_to, StreamEnd.for_attachment(self(), doc_id, filename))
        :ok
      {:ibrowse_async_response_timeout, ^req_id} ->
        send(origin, {__MODULE__, self(), :timeout, []})
        :ok
    end
  end

  defp transform_document_multipart(origin, doc_id, stream_to, {:init, boundary} = multipart, buffer, req_id) do
    case ICouch.Multipart.split_part(buffer, boundary) do
      {"", nil, _} ->
        transform_document(origin, doc_id, stream_to, multipart, buffer, req_id)
      {"", _, rest} when rest != nil ->
        transform_document_multipart(origin, doc_id, stream_to, {:document, "", boundary}, rest, req_id)
      _ ->
        transform_document(origin, doc_id, stream_to, :end, "", req_id)
    end
  end
  defp transform_document_multipart(origin, doc_id, stream_to, {:document, doc_buffer, boundary}, buffer, req_id) do
    case ICouch.Multipart.split_part(buffer, boundary) do
      {_, _, nil} ->
        transform_document(origin, doc_id, stream_to, :end, "", req_id)
      {data, next_headers, rest} ->
        doc_buffer = doc_buffer <> data
        if next_headers != nil do
          decode_send_doc(doc_id, stream_to, doc_buffer)
          case Regex.run(~r/attachment; *filename="([^"]*)"/, Map.get(next_headers, "content-disposition", "")) do
            [_, filename] ->
              transform_document_multipart(origin, doc_id, stream_to, {:attachment, filename, boundary}, rest, req_id)
            _ ->
              transform_document_multipart(origin, doc_id, stream_to, {:attachment, nil, boundary}, rest, req_id)
          end
        else
          transform_document(origin, doc_id, stream_to, {:document, doc_buffer, boundary}, rest, req_id)
        end
    end
  end
  defp transform_document_multipart(origin, doc_id, stream_to, {:attachment, filename, boundary} = multipart, buffer, req_id) do
    case ICouch.Multipart.split_part(buffer, boundary) do
      {data, next_headers, rest} ->
        if byte_size(data) > 0 do
          send(stream_to, StreamChunk.for_attachment(self(), doc_id, filename, data))
        end
        if next_headers != nil or rest == nil do
          send(stream_to, StreamEnd.for_attachment(self(), doc_id, filename))
        end
        cond do
          next_headers != nil ->
            case Regex.run(~r/attachment; *filename="([^"]*)"/, Map.get(next_headers, "content-disposition", "")) do
              [_, filename] ->
                transform_document_multipart(origin, doc_id, stream_to, {:attachment, filename, boundary}, rest, req_id)
              _ ->
                transform_document_multipart(origin, doc_id, stream_to, {:attachment, nil, boundary}, rest, req_id)
            end
          rest == nil ->
            transform_document(origin, doc_id, stream_to, :end, "", req_id)
          true ->
            transform_document(origin, doc_id, stream_to, multipart, rest, req_id)
        end
    end
  end

  defp decode_send_doc(doc_id, stream_to, buffer) do
    case ICouch.Document.from_api(buffer) do
      {:ok, %{attachment_data: atts} = doc} when map_size(atts) > 0 ->
        send(stream_to, StreamChunk.for_document(self(), doc_id, %{doc | attachment_data: %{}}))
        Enum.each(atts, fn ({att_name, att_data}) ->
          send(stream_to, StreamChunk.for_attachment(self(), doc_id, att_name, att_data))
          send(stream_to, StreamEnd.for_attachment(self(), doc_id, att_name))
        end)
      {:ok, doc} ->
        send(stream_to, StreamChunk.for_document(self(), doc_id, doc))
      _ ->
        send(stream_to, StreamChunk.for_document(self(), doc_id, nil))
        :ok
    end
  end

  @doc false
  def collect_document(ref, timeout) do
    receive do
      %StreamChunk{ref: ^ref, type: :document, data: doc} ->
        collect_attachments(ref, doc, timeout)
    after timeout ->
      cancel(ref)
      {:error, :timeout}
    end
  end

  defp collect_attachments(ref, doc, timeout) do
    receive do
      %StreamChunk{ref: ^ref, type: :attachment, name: {_, name}, data: data} ->
        case doc do
          %{attachment_data: %{^name => existing_data} = attachment_data} ->
            collect_attachments(ref, %{doc | attachment_data: %{attachment_data | name => existing_data <> data}}, timeout)
          %{attachment_data: attachment_data} ->
            collect_attachments(ref, %{doc | attachment_data: Map.put(attachment_data, name, data)}, timeout)
        end
      %StreamEnd{ref: ^ref, type: :attachment} ->
        collect_attachments(ref, doc, timeout)
      %StreamEnd{ref: ^ref, type: :document} ->
        {:ok, doc}
    after timeout ->
      cancel(ref)
      {:error, :timeout}
    end
  end

  @doc false
  def collect_attachment(ref, timeout),
    do: collect_attachment(ref, "", timeout)

  defp collect_attachment(ref, buffer, timeout) do
    receive do
      %StreamChunk{ref: ^ref, type: :attachment, data: data} ->
        collect_attachment(ref, buffer <> data, timeout)
      %StreamEnd{ref: ^ref, type: :attachment} ->
        {:ok, buffer}
    after timeout ->
      cancel(ref)
      {:error, :timeout}
    end
  end
end
