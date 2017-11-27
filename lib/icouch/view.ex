
# Created by Patrick Schneider on 28.08.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule ICouch.View do
  @moduledoc """
  Module to handle views in CouchDB.

  View structs should not be created or manipulated directly, please use
  `ICouch.open_view/3` or `ICouch.open_view!/3`.

  The view struct implements the enumerable protocol for easy handling with
  Elixir's `Enum` module.

  A view can be in a "fetched" state or in an "unfetched" state which can be
  tested with the `fetched?/1` function and changed with the `fetch/1`,
  `fetch!/1` and `unfetch/1` function.

  Note that iterating over an unfetched view will create an intermediate fetched
  version.
  """

  use ICouch.RequestError

  defstruct [:db, :ddoc, :name, :params, :rows, :total_rows, :update_seq]

  @type t :: %__MODULE__{
    db: ICouch.DB.t,
    ddoc: String.t | nil,
    name: String.t,
    params: map,
    rows: [map] | nil,
    total_rows: integer | nil,
    update_seq: integer | String.t | nil
  }

  @type view_option_key :: :conflicts | :descending | :endkey | :endkey_docid |
    :group | :group_level | :include_docs | :attachments | :att_encoding_info |
    :inclusive_end | :key | :keys | :limit | :reduce | :skip | :stale |
    :startkey | :startkey_docid | :update_seq

  @type view_option_value :: boolean | String.t | integer | [String.t] | :ok |
    :update_after

  @doc """
  Fetches all rows of `view`, turning it into a "fetched view".
  """
  @spec fetch(view :: t) :: {:ok, t} | {:error, term}
  def fetch(%__MODULE__{params: params} = view) do
    case send_req(view) do
      {:ok, %{"rows" => rows} = result} ->
        rows = if params[:include_docs] do
          Enum.map(rows, fn
            %{"doc" => doc} = row when doc != nil ->
              %{row | "doc" => ICouch.Document.from_api!(doc)}
            other ->
              other
          end)
        else
          rows
        end
        {:ok, %{view | rows: rows, total_rows: result["total_rows"], update_seq: result["update_seq"]}}
      {:ok, _} ->
        {:error, :invalid_response}
      other ->
        other
    end
  end

  @doc """
  Same as `fetch/1` but returns the fetched view directly on success or raises
  an error on failure.
  """
  @spec fetch!(view :: t) :: t
  def fetch!(view),
    do: req_result_or_raise! fetch(view)

  @doc """
  Resets `view` back to the "unfetched" state.
  """
  @spec unfetch(view :: t) :: t
  def unfetch(%__MODULE__{} = view),
    do: %{view | rows: nil}

  @doc """
  Tests whether `view` is in "fetched" state or not.
  """
  @spec fetched?(view :: t) :: boolean
  def fetched?(%__MODULE__{rows: rows}) when is_list(rows),
    do: true
  def fetched?(%__MODULE__{}),
    do: false

  @doc """
  Replaces `view`'s options with the given ones.

  This will also set the view back to the "unfetched" state.
  """
  @spec set_options(view :: t, options :: [ICouch.open_view_option]) :: t
  def set_options(%__MODULE__{} = view, options),
    do: %{view | params: Map.new(options), rows: nil}

  @doc """
  Adds or updates a single option in `view`.

  This will also set the view back to the "unfetched" state.
  """
  @spec put_option(view :: t, key :: view_option_key, value :: view_option_value) :: t
  def put_option(%__MODULE__{params: params} = view, key, value),
    do: %{view | params: Map.put(params, key, value), rows: nil}

  @doc """
  Deletes an option in `view`.

  This will also set the view back to the "unfetched" state.

  Returns `view` unchanged if the option was not set (and it already was
  "unfetched").
  """
  @spec delete_option(view :: t, key :: view_option_key) :: t
  def delete_option(%__MODULE__{params: params, rows: rows} = view, key) do
    if not Map.has_key?(params, key) and rows == nil do
      view
    else
      %{view | params: Map.delete(params, key), rows: nil}
    end
  end

  @doc """
  Returns the value of an option in `view` or `nil` if it was not set.
  """
  @spec get_option(view :: t, key :: view_option_key) :: view_option_value | nil
  def get_option(%__MODULE__{params: params}, key),
    do: Map.get(params, key)

  @doc """
  Internal function to build a db endpoint.
  """
  @spec db_endpoint(view :: t) :: {String.t, map}
  def db_endpoint(%__MODULE__{ddoc: ddoc, name: name, params: params}),
    do: db_endpoint(ddoc, name, params)

  defp send_req(%{db: db, ddoc: ddoc, name: name, params: %{keys: keys} = params}),
    do: ICouch.DB.send_req(db, db_endpoint(ddoc, name, Map.delete(params, :keys)), :post, %{"keys" => keys})
  defp send_req(%{db: db, ddoc: ddoc, name: name, params: params}),
    do: ICouch.DB.send_req(db, db_endpoint(ddoc, name, params))

  defp db_endpoint(nil, "_all_docs", params),
    do: {"_all_docs", params}
  defp db_endpoint(ddoc, name, params),
    do: {"_design/#{ddoc}/_view/#{name}", params}
end

defimpl Enumerable, for: ICouch.View do
  def count(%ICouch.View{params: params} = view) do
    %{rows: rows} = ICouch.View.fetch!(%{view | params: Map.delete(params, :include_docs)})
    {:ok, length(rows)}
  end

  def member?(_view, _element),
    do: {:error, __MODULE__}

  def reduce(_, {:halt, acc}, _fun),
    do: {:halted, acc}
  def reduce(%ICouch.View{rows: rest_rows}, {:suspend, acc}, fun),
    do: {:suspended, acc, &reduce(rest_rows, &1, fun)}
  def reduce(%ICouch.View{rows: []}, {:cont, acc}, _fun),
    do: {:done, acc}
  def reduce(%ICouch.View{rows: [h | t]} = view, {:cont, acc}, fun),
    do: reduce(%{view | rows: t}, fun.(h, acc), fun)
  def reduce(%ICouch.View{rows: nil} = view, acc, fun),
    do: ICouch.View.fetch!(view) |> reduce(acc, fun)
end
