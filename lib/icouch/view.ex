
# Created by Patrick Schneider on 28.08.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule ICouch.View do
  @moduledoc """
  Module to handle views in CouchDB.

  The view struct implements the enumerable protocol for easy handling with
  Elixir's `Enum` module.
  """

  use ICouch.RequestError

  defstruct [:db, :ddoc, :name, :params, :rows]

  @type t :: %__MODULE__{
    db: ICouch.DB.t,
    ddoc: String.t | nil,
    name: String.t,
    params: map,
    rows: [map] | nil
  }

  @doc """
  Fetches all rows of this view, turning it into a "fetched view".
  """
  def fetch(%__MODULE__{params: params} = view) do
    case send_req(view) do
      {:ok, %{"rows" => rows}} ->
        if params[:include_docs] do
          {:ok, %{view | rows: (for %{"doc" => doc} = row <- rows, do: %{row | "doc" => ICouch.Document.from_api!(doc)})}}
        else
          {:ok, %{view | rows: rows}}
        end
      {:ok, _} ->
        {:error, :invalid_response}
      other ->
        other
    end
  end

  def fetch!(view),
    do: req_result_or_raise! fetch(view)

  def fetched?(%__MODULE__{rows: rows}) when is_list(rows),
    do: true
  def fetched?(%__MODULE__{}),
    do: false

  def db_endpoint(%__MODULE__{ddoc: nil, name: "_all_docs", params: params}),
    do: {"_all_docs", params}
  def db_endpoint(%__MODULE__{ddoc: ddoc, name: name, params: params}),
    do: {"_design/#{ddoc}/_view/#{name}", params}

  defp send_req(%{db: db, params: %{keys: keys}} = view),
    do: ICouch.DB.send_req(db, db_endpoint(view), :post, %{"keys" => keys})
  defp send_req(%{db: db} = view),
    do: ICouch.DB.send_req(db, db_endpoint(view))
end

defimpl Enumerable, for: ICouch.View do
  def count(%ICouch.View{params: params} = view) do
    case ICouch.View.fetch(%{view | params: Map.delete(params, :include_docs)}) do
      {:ok, %{rows: rows}} ->
        {:ok, length(rows)}
      other ->
        other
    end
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
  def reduce(%ICouch.View{rows: nil} = view, acc, fun) do
    case ICouch.View.fetch(view) do
      {:ok, fetched_view} ->
        reduce(fetched_view, acc, fun)
      other ->
        other
    end
  end
end
