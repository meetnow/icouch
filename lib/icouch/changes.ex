
# Created by Patrick Schneider on 30.08.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule ICouch.Changes do
  @moduledoc """
  Module to handle changes feeds in CouchDB.

  Changes structs should not be created or manipulated directly, please use
  `ICouch.open_changes/2`.

  Similar to a view, a changes feed can be in a "fetched" state or in an
  "unfetched" state which can be tested with the `fetched?/1` function and
  changed with the `fetch/1`, `fetch!/1` and `unfetch/1` function. In contrast
  to a view, the sequence number is updated on each fetch so a consecutive
  fetch will start off at the last sequence number.

  The changes struct implements the enumerable protocol for easy handling with
  Elixir's `Enum` module - however, this only works with fetched changes and
  will fail with an `ArgumentError` otherwise.
  """

  use ICouch.RequestError

  defstruct [:db, :last_seq, :params, :results]

  @type t :: %__MODULE__{
    db: ICouch.DB.t,
    last_seq: String.t | integer | nil,
    params: map,
    results: [map] | nil
  }

  @type changes_option_key :: :doc_ids | :conflicts | :descending |
    :filter | :include_docs | :attachments | :att_encoding_info | :limit |
    :since | :style | :view

  @type changes_option_value :: boolean | String.t | integer | [String.t] |
    :main_only | :all_docs

  @doc """
  Fetches all results of `changes`, turning it into a "fetched changes feed".

  The last sequence number will be set and used as next "since" parameter.
  """
  @spec fetch(changes :: t) :: {:ok, t} | {:error, term}
  def fetch(%__MODULE__{params: params} = changes) do
    case send_req(changes) do
      {:ok, %{"results" => results, "last_seq" => last_seq}} ->
        if params[:include_docs] do
          {:ok, %{changes | last_seq: last_seq, results: (for %{"doc" => doc} = row <- results, do: %{row | "doc" => ICouch.Document.from_api!(doc)})}}
        else
          {:ok, %{changes | last_seq: last_seq, results: results}}
        end
      {:ok, _} ->
        {:error, :invalid_response}
      other ->
        other
    end
  end

  @doc """
  Same as `fetch/1` but returns the fetched changes feed directly on success or
  raises an error on failure.
  """
  @spec fetch!(changes :: t) :: t
  def fetch!(changes),
    do: req_result_or_raise! fetch(changes)

  @doc """
  Resets `changes` back to the "unfetched" state.

  This will also reset the `last_seq` to `nil`.
  """
  @spec unfetch(changes :: t) :: t
  def unfetch(%__MODULE__{} = changes),
    do: %{changes | last_seq: nil, results: nil}

  @doc """
  Tests whether `changes` is in "fetched" state or not.
  """
  @spec fetched?(changes :: t) :: boolean
  def fetched?(%__MODULE__{results: results}) when is_list(results),
    do: true
  def fetched?(%__MODULE__{}),
    do: false

  @doc """
  Replaces `changes`'s options with the given ones.

  This set the changes feed back to the "unfetched" state, but leaves the
  `last_seq` value untouched unless `since` is given as option.
  """
  @spec set_options(changes :: t, options :: [ICouch.open_changes_option]) :: t
  def set_options(%__MODULE__{} = changes, options) do
    case Map.new(options) do
      %{feed: _} ->
        raise ArgumentError, message: "the \"feed\" option is not allowed here"
      options ->
        case Map.pop(options, :since) do
          {nil, options} -> %{changes | params: options, results: nil}
          {since, options} -> %{changes | last_seq: since, params: options, results: nil}
        end
    end
  end

  @doc """
  Adds or updates a single option in `changes`.

  This will also set the changes feed back to the "unfetched" state. To modify
  the `last_seq` value, set the `since` option.
  """
  @spec put_option(changes :: t, key :: changes_option_key, value :: changes_option_value) :: t
  def put_option(%__MODULE__{}, :feed, _),
    do: raise ArgumentError, message: "the \"feed\" option is not allowed here"
  def put_option(%__MODULE__{params: params} = changes, key, value),
    do: %{changes | params: Map.put(params, key, value), results: nil}

  @doc """
  Deletes an option in `changes`.

  This will also set the changes feed back to the "unfetched" state.

  Returns `changes` unchanged if the option was not set (and it already was
  "unfetched").
  """
  @spec delete_option(changes :: t, key :: changes_option_key) :: t
  def delete_option(%__MODULE__{params: params, results: results} = changes, key) do
    if not Map.has_key?(params, key) and results == nil do
      changes
    else
      %{changes | params: Map.delete(params, key), results: nil}
    end
  end

  @doc """
  Returns the value of an option in `changes` or `nil` if it was not set.

  The `last_seq` value can be retrieved with the `since` option.
  """
  @spec get_option(changes :: t, key :: changes_option_key) :: changes_option_value | nil
  def get_option(%__MODULE__{last_seq: last_seq}, :since),
    do: last_seq
  def get_option(%__MODULE__{params: params}, key),
    do: Map.get(params, key)

  @doc """
  Internal function to build a db endpoint.
  """
  @spec db_endpoint(changes :: t) :: {String.t, map}
  def db_endpoint(%__MODULE__{last_seq: last_seq, params: params}),
    do: db_endpoint(last_seq, params)

  defp send_req(%{db: db, last_seq: last_seq, params: %{doc_ids: doc_ids} = params}),
    do: ICouch.DB.send_req(db, db_endpoint(last_seq, Map.delete(params, :doc_ids) |> Map.put(:filter, "_doc_ids")), :post, %{"doc_ids" => doc_ids})
  defp send_req(%{db: db, last_seq: last_seq, params: params}),
    do: ICouch.DB.send_req(db, db_endpoint(last_seq, params))

  defp db_endpoint(nil, params),
    do: {"_changes", params}
  defp db_endpoint(last_seq, params),
    do: {"_changes", Map.put(params, :since, last_seq)}
end

defimpl Enumerable, for: ICouch.Changes do
  def count(%ICouch.Changes{results: nil}),
    do: raise ArgumentError, message: "changes feed not fetched"
  def count(%ICouch.Changes{results: results}),
    do: {:ok, length(results)}

  def member?(_changes, _element),
    do: {:error, __MODULE__}

  def reduce(_, {:halt, acc}, _fun),
    do: {:halted, acc}
  def reduce(%ICouch.Changes{results: rest_results}, {:suspend, acc}, fun),
    do: {:suspended, acc, &reduce(rest_results, &1, fun)}
  def reduce(%ICouch.Changes{results: []}, {:cont, acc}, _fun),
    do: {:done, acc}
  def reduce(%ICouch.Changes{results: [h | t]} = changes, {:cont, acc}, fun),
    do: reduce(%{changes | results: t}, fun.(h, acc), fun)
  def reduce(%ICouch.Changes{results: nil}, _, _),
    do: raise ArgumentError, message: "changes feed not fetched"
end
