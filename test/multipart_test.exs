
# Created by Patrick Schneider on 01.09.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule MultipartTest do
  use ExUnit.Case

  alias ICouch.Multipart

  test "get_boundary" do
    assert Multipart.get_boundary([
      {'Age', '3600'},
      {'Content-Type', 'multipart/mixed;boundary="bound"'}]) ===
      {:ok, "mixed", "bound"}
    assert Multipart.get_boundary([
      {'content-type', 'multipart/related;      boundary="--what"'}]) ===
      {:ok, "related", "--what"}
    assert Multipart.get_boundary([
      {"Content-Type", "multipart/unrelated; boundary=\"test\""}]) ===
      {:ok, "unrelated", "test"}
    assert Multipart.get_boundary(
      %{"content-type" => "multipart/youknowit; boundary=\"valid\""}) ===
      {:ok, "youknowit", "valid"}
    assert Multipart.get_boundary(
      %{"Content-Type" => "multipart/youknowit; boundary=\"valid\""}) ===
      {:ok, "youknowit", "valid"}

    assert Multipart.get_boundary([{'Nope', 'nothing here'}]) === nil
    assert Multipart.get_boundary(%{"Nope" => "nothing here"}) === nil

    assert Multipart.get_boundary([
      {'Content-Type', 'multipart/mixed; bondary="10k bond"'}]) === nil
    assert Multipart.get_boundary([
      {'Content-Type', 'multipart/mixed, boundary="nothowitworks"'}]) === nil
  end

  test "split_part" do
    assert Multipart.split_part("too short", "boundary") ===
      {"", nil, "too short"}

    assert Multipart.split_part("--x--", "x") === {"", nil, nil}
    assert Multipart.split_part("pre--x--", "x") === {"", nil, "pre--x--"}
    assert Multipart.split_part("pre\r\n--x--", "x") === {"pre", nil, nil}
    assert Multipart.split_part("pre\n--x--post", "x") === {"pre", nil, nil}

    nulls = Enum.join(Enum.reduce(1..1024,[],fn(_,a)->["0"|a]end),"")

    assert Multipart.split_part("pre\n--x", "x") === {"", nil, "pre\n--x"}
    assert Multipart.split_part("pre\n--x\n", "x") === {"pre", nil, "--x\n"}
    assert Multipart.split_part("pre\n--x\r\n", "x") === {"pre", nil, "--x\r\n"}
    assert Multipart.split_part("pre\r\n--x\r\n", "x") ===
      {"pre", nil, "--x\r\n"}
    assert Multipart.split_part(nulls <> "11111", "x") ===
      {"", nil, nulls <> "11111"}
    assert Multipart.split_part(nulls <> "111111", "x") ===
      {nulls, nil, "111111"}

    assert Multipart.split_part("pre\r\n--x\r\n\r\npost", "x") ===
      {"pre", %{}, "post"}
    assert Multipart.split_part("pre\n--x\n\npost", "x") ===
      {"pre", %{}, "post"}
    assert Multipart.split_part("--x\nKEY: Value\n\n", "x") ===
      {"", %{"key" => "Value"}, ""}
  end

  test "join_part" do
    assert ICouch.Multipart.join_part(nil, nil, "x") === "--x--"

    assert Multipart.join_part(nil, "data", "x") === "--x\r\n\r\ndata\r\n"
    assert Multipart.join_part(%{}, "data", "x") === "--x\r\n\r\ndata\r\n"
    assert Multipart.join_part([], "data", "x") === "--x\r\n\r\ndata\r\n"

    assert Multipart.join_part([Age: "3600"], "data", "x") ===
      "--x\r\nAge: 3600\r\n\r\ndata\r\n"
    assert Multipart.join_part(%{"Age" => "3600"}, "data", "x") ===
      "--x\r\nAge: 3600\r\n\r\ndata\r\n"
    assert Multipart.join_part([
      {"Age", "3600"}, {"Content-Type", "text/plain"}], "data", "x") ===
      "--x\r\nAge: 3600\r\nContent-Type: text/plain\r\n\r\ndata\r\n"
  end

  test "split and join" do
    multipart_data = "--MultipartBoundary\r\nContent-Type: text/plain\r\n\r\nhello world\r\n--MultipartBoundary\r\nContent-Type: application/json\r\n\r\n{\"key\":\"value\"}\r\n--MultipartBoundary--"

    assert Multipart.split("--x--", "x") === {:ok, []}

    assert Multipart.split("--x-", "x") === :error
    assert Multipart.split("--x\n\ndata\n--x-", "x") === :error

    assert Multipart.split(multipart_data, "MultipartBoundary") ===
      {:ok, [{%{"content-type" => "text/plain"}, "hello world"},
        {%{"content-type" => "application/json"}, "{\"key\":\"value\"}"}]}

    assert Multipart.join([], "x") === "--x--"

    assert Multipart.join([
        {%{"Content-Type" => "text/plain"}, "hello world"},
        {%{"Content-Type" => "application/json"}, "{\"key\":\"value\"}"}],
      "MultipartBoundary") === multipart_data

  end
end
