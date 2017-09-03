
# Created by Patrick Schneider on 03.09.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule OfflineTest do
  use ExUnit.Case

  test "endpoint options" do
    # Easy
    assert ICouch.Server.endpoint_with_options("point") ===
      %URI{authority: nil, fragment: nil, host: nil, path: "point",
        port: nil, query: nil, scheme: nil, userinfo: nil}

    # Plain + Batch
    assert ICouch.Server.endpoint_with_options("point", rev: "A", filter: "B",
      view: "C", since: "D", startkey_docid: "E", endkey_docid: "F",
      batch: true) ===
      %URI{authority: nil, fragment: nil, host: nil, path: "point", port: nil,
        query: "rev=A&filter=B&view=C&since=D&startkey_docid=E&endkey_docid=F&batch=ok",
        scheme: nil, userinfo: nil}

    # Remove/Cancel
    assert ICouch.Server.endpoint_with_options("point", multipart: true,
      stream_to: :pid, since: nil, batch: false) ===
      %URI{authority: nil, fragment: nil, host: nil, path: "point",
        port: nil, query: nil, scheme: nil, userinfo: nil}

    # Quote/Atom
    assert ICouch.Server.endpoint_with_options("point",
      key: %{"a" => %{"b" => "c"}}, stale: :ok, doc_ids: ["a", "b"]) ===
      %URI{authority: nil, fragment: nil, host: nil, path: "point", port: nil,
        query: "key=%7B%22a%22%3A%7B%22b%22%3A%22c%22%7D%7D&stale=ok&doc_ids=%5B%22a%22%2C%22b%22%5D",
        scheme: nil, userinfo: nil}

    # Query Params
    assert ICouch.Server.endpoint_with_options("point", since: 1,
      query_params: %{since: 2, thing: "thong", rev: "3"}, rev: 4) ===
      %URI{authority: nil, fragment: nil, host: nil, path: "point", port: nil,
        query: "since=1&rev=3&thing=thong&rev=4", scheme: nil, userinfo: nil}

    # DB->Server Endpoint (rare case)
    assert ICouch.DB.server_endpoint(%ICouch.DB{name: "my_db"},
      URI.parse("my_path")) ===
      %URI{authority: nil, fragment: nil, host: nil, path: "my_db/my_path",
        port: nil, query: nil, scheme: nil, userinfo: nil}
  end

  test "server connection setup" do
    # Connect default
    assert ICouch.server_connection() === %ICouch.Server{
      direct: nil, ib_options: [], timeout: nil, uri: %URI{
        authority: "127.0.0.1:5984", fragment: nil, host: "127.0.0.1",
        path: nil, port: 5984, query: nil, scheme: "http", userinfo: nil}}

    # Connect specific with auth
    assert ICouch.server_connection("http://admin:admin@192.168.99.100:8000/") ===
      %ICouch.Server{direct: nil, ib_options: [basic_auth: {'admin', 'admin'}],
        timeout: nil, uri: %URI{authority: "192.168.99.100", fragment: nil,
          host: "192.168.99.100", path: "/", port: 8000, query: nil,
          scheme: "http", userinfo: nil}}

    # Connect special
    assert ICouch.server_connection("http://example.com/", timeout: 5_000,
      cookie: "my=cookie", direct_conn_pid: :a_process, max_sessions: 5,
      max_attempts: 2) ===
      %ICouch.Server{direct: :a_process, ib_options: [max_attempts: 2,
          max_sessions: 5, cookie: 'my=cookie'],
        timeout: 5000, uri: %URI{authority: "example.com", fragment: nil,
          host: "example.com", path: "/", port: 80, query: nil, scheme: "http",
          userinfo: nil}}
  end

  test "document manipulation" do
    # Creating
    assert ICouch.Document.new() === %ICouch.Document{id: nil, rev: nil,
      fields: %{}, attachment_order: [], attachment_data: %{}}

    assert ICouch.Document.new("my_id", nil) === %ICouch.Document{id: "my_id",
      rev: nil, fields: %{"_id" => "my_id"}, attachment_order: [],
      attachment_data: %{}}

    assert ICouch.Document.new("my_id", "my_rev") === %ICouch.Document{
      id: "my_id", rev: "my_rev",
      fields: %{"_id" => "my_id", "_rev" => "my_rev"}, attachment_order: [],
      attachment_data: %{}}

    fields = %{"_id" => "my_id", "_rev" => "my_rev", "k" => "v"}
    simple_doc = %ICouch.Document{id: "my_id", rev: "my_rev", fields: fields,
      attachment_order: [], attachment_data: %{}}
    assert ICouch.Document.new(fields) === simple_doc

    # Deserialization (simple)
    simple_doc_bin = ~s({"k":"v","_rev":"my_rev","_id":"my_id"})
    assert ICouch.Document.from_api!(simple_doc_bin) === simple_doc

    assert ICouch.Document.from_api("garbage") === {:error, {:invalid, "g", 0}}
    assert_raise Poison.SyntaxError, fn -> ICouch.Document.from_api!("garbage") end

    # Serialization (simple)
    assert ICouch.Document.to_api!(simple_doc) === simple_doc_bin

    # Deserialization (base64)
    att_data_plain = "compress me compress me compress me compress me compress me"
    att_data = Base.encode64(att_data_plain)
    att_hash = :crypto.hash(:md5, att_data_plain) |> Base.encode64()
    document_data = ~s({"k":"v","_rev":"my_rev","_id":"my_id","_attachments":{"test.txt":{"revpos":1,"digest":"md5-#{att_hash}","data":"#{att_data}","content_type":"text/plain"}}})
    document = %ICouch.Document{id: "my_id", rev: "my_rev",
      attachment_data: %{"test.txt" => att_data_plain},
      attachment_order: ["test.txt"], fields: %{
        "_id" => "my_id", "_rev" => "my_rev", "k" => "v", "_attachments" => %{
          "test.txt" => %{
            "content_type" => "text/plain", "digest" => "md5-#{att_hash}",
            "length" => byte_size(att_data_plain), "revpos" => 1,
            "stub" => true}}}}

    assert ICouch.Document.from_api(document_data) === {:ok, document}

    # Deserialization (broken attachment data; leads to a non-api-compliant document)
    noncompliant_document = %ICouch.Document{id: "my_id", rev: "my_rev",
      attachment_data: %{}, attachment_order: ["test.txt"], fields: %{
        "_id" => "my_id", "_rev" => "my_rev",
        "_attachments" => %{"test.txt" => %{"data" => "garbage"}}}}
    noncompliant_document_data = ~s({"_rev":"my_rev","_id":"my_id","_attachments":{"test.txt":{"data":"garbage"}}})
    assert ICouch.Document.from_api(noncompliant_document_data) === {:ok, noncompliant_document}

    # Serialization (broken attachment data; still safe to use, depends on server implementation)
    assert ICouch.Document.to_api(noncompliant_document) === {:ok, noncompliant_document_data}

    # Serialization (base64)
    assert ICouch.Document.to_api(document) === {:ok, document_data}
    assert_raise ArgumentError, "document attachments inconsistent", fn -> ICouch.Document.to_api(%{document | attachment_order: []}) end
    assert_raise ArgumentError, "document attachments inconsistent", fn -> ICouch.Document.to_api(%{document | attachment_order: ["garbage", "fail"]}) end

    # Serialization (base64; pretty; through Poison API)
    assert Poison.encode!(document, pretty: true) <> "\n" ===
    """
    {
      "k": "v",
      "_rev": "my_rev",
      "_id": "my_id",
      "_attachments": {
        "test.txt": {
          "revpos": 1,
          "digest": "md5-zN8PL0SwCF9udv9StYegpA==",
          "data": "Y29tcHJlc3MgbWUgY29tcHJlc3MgbWUgY29tcHJlc3MgbWUgY29tcHJlc3MgbWUgY29tcHJlc3MgbWU=",
          "content_type": "text/plain"
        }
      }
    }
    """

    # Deserialization (multipart/gzip)
    att_data = :zlib.gzip(att_data_plain)
    att_hash = :crypto.hash(:md5, att_data) |> Base.encode64()
    att_info = %{
      "content_type" => "text/plain", "digest" => "md5-#{att_hash}",
      "encoded_length" => byte_size(att_data), "encoding" => "gzip",
      "length" => byte_size(att_data_plain), "revpos" => 1,
      "stub" => true
    }
    document_data_plain = """
    {
      "_id": "my_id",
      "_rev": "my_rev",
      "k": "v",
      "_attachments": {
        "test.txt": {
          "content_type": "text/plain",
          "revpos": 1,
          "digest": "md5-#{att_hash}",
          "length": #{byte_size(att_data_plain)},
          "follows": true,
          "encoding": "gzip",
          "encoded_length": #{byte_size(att_data)}
        }
      }
    }
    """
    document_data = :zlib.gzip(document_data_plain)
    document = %ICouch.Document{id: "my_id", rev: "my_rev",
      attachment_data: %{"test.txt" => att_data_plain},
      attachment_order: ["test.txt"], fields: %{
        "_id" => "my_id", "_rev" => "my_rev", "k" => "v", "_attachments" => %{
          "test.txt" => att_info}}}

    assert ICouch.Document.from_multipart([
      {%{"content-type" => "application/json", "content-encoding" => "gzip",
        "content-length" => "#{byte_size(document_data)}"}, document_data},
      {%{"content-disposition" => "attachment; filename=\"test.txt\"",
        "content-type" => "text/plain", "content-encoding" => "gzip",
        "content-length" => "#{byte_size(att_data)}"}, att_data}
    ]) === {:ok, document}

    # Tolerate false content encoding for document;
    # discard falsely encoded attachment; ignore non-attachment parts
    assert ICouch.Document.from_multipart([
      {%{"content-type" => "application/json", "content-encoding" => "gzip",
        "content-length" => "#{byte_size(document_data_plain)}"}, document_data_plain},
      {%{"content-disposition" => "attachment; filename=\"test.txt\"",
        "content-type" => "text/plain", "content-encoding" => "gzip",
        "content-length" => "1"}, "x"},
      {%{"broken" => "true"}, "garbage"}
    ]) === {:ok, %{document | attachment_data: %{}}}

    assert ICouch.Document.from_multipart([
        {%{"content-type" => "application/json"}, "garbage"}]) ===
      {:error, {:invalid, "g", 0}}

    # Add attachment
    att2_data = <<1, 2, 3, 4, 5, 6, 7>>
    att2_hash = :crypto.hash(:md5, att2_data) |> Base.encode64()
    att2_info = %{
      "content_type" => "application/octet-stream",
      "digest" => "md5-#{att2_hash}", "length" => byte_size(att2_data),
      "stub" => true
    }
    document2 = %ICouch.Document{id: "my_id", rev: "my_rev",
      attachment_data: %{"anew.dat" => att2_data, "test.txt" => att_data_plain},
      attachment_order: ["test.txt", "anew.dat"], fields: %{
        "_id" => "my_id", "_rev" => "my_rev", "k" => "v", "_attachments" => %{
          "anew.dat" => att2_info,
          "test.txt" => att_info}}}
    assert ICouch.Document.put_attachment(document, "anew.dat", att2_data,
      "application/octet-stream", "md5-#{att2_hash}") ===
      document2

    # Add attachment (using defaults)
    assert ICouch.Document.put_attachment(document, "anew.dat", att2_data) ===
      %ICouch.Document{id: "my_id", rev: "my_rev",
      attachment_data: %{"anew.dat" => att2_data, "test.txt" => att_data_plain},
      attachment_order: ["test.txt", "anew.dat"], fields: %{
        "_id" => "my_id", "_rev" => "my_rev", "k" => "v", "_attachments" => %{
          "anew.dat" => %{
            "content_type" => "application/octet-stream",
            "length" => byte_size(att2_data), "stub" => true
          },
          "test.txt" => att_info}}}

    # Serialization (multipart)
    assert ICouch.Document.to_multipart(document2) === {:ok, [
      {%{"Content-Type" => "application/json"}, ~s({"k":"v","_rev":"my_rev","_id":"my_id","_attachments":{"test.txt":{"revpos":1,"length":#{byte_size(att_data_plain)},"follows":true,"encoding":"gzip","encoded_length":#{byte_size(att_data)},"digest":"md5-#{att_hash}","content_type":"text/plain"},"anew.dat":{"length":#{byte_size(att2_data)},"follows":true,"digest":"md5-#{att2_hash}","content_type":"application/octet-stream"}}})},
      {%{"Content-Disposition" => "attachment; filename=\"test.txt\""}, att_data_plain},
      {%{"Content-Disposition" => "attachment; filename=\"anew.dat\""}, att2_data}]}

    assert ICouch.Document.to_multipart(%ICouch.Document{fields: %{fail: {:tuple}}}) ===
      {:error, {:invalid, {:tuple}}}

    # Delete attachment
    assert ICouch.Document.delete_attachment(document2, "anew.dat") === document
    assert ICouch.Document.delete_attachment(simple_doc, "garbage") === simple_doc

    # Delete attachments
    assert ICouch.Document.delete_attachments(document2) === simple_doc
    assert ICouch.Document.delete_attachments(document) === simple_doc

    # Delete attachment data
    assert ICouch.Document.delete_attachment_data(document2) ===
      %{document2 | attachment_data: %{}}
    assert ICouch.Document.delete_attachment_data(document2, "anew.dat") ===
      %{document2 | attachment_data: document.attachment_data}

    # Put attachment data (invalid)
    assert_raise KeyError, fn -> ICouch.Document.put_attachment_data(simple_doc, "test.txt", "data") end

    # Put attachment info and data (valid)
    assert ICouch.Document.put_attachment_info(document, "anew.dat", att2_info) |>
      ICouch.Document.put_attachment_data("anew.dat", att2_data) === document2

    # Get attachment info and data
    assert ICouch.Document.get_attachment_info(document2, "anew.dat") ===
      att2_info
    assert ICouch.Document.get_attachment_data(document, "test.txt") ===
      att_data_plain

    # Has attachment / has attachment data
    assert ICouch.Document.has_attachment?(document, "test.txt")
    assert ICouch.Document.has_attachment_data?(document, "test.txt")
    refute ICouch.Document.has_attachment?(simple_doc, "test.txt")

    # Get attachment
    assert ICouch.Document.get_attachment(document2, "anew.dat") ===
      {att2_info, att2_data}
    assert ICouch.Document.get_attachment(document, "test.txt") ===
      {att_info, att_data_plain}
    assert ICouch.Document.get_attachment(document, "anew.dat") === nil

    # Iterate
    assert Enum.to_list(simple_doc) === [{"_id", "my_id"}, {"_rev", "my_rev"}, {"k", "v"}]

    # Determine byte size
    assert ICouch.Document.json_byte_size(simple_doc) === byte_size(Poison.encode!(simple_doc))
    assert ICouch.Document.attachment_data_size(document2) === byte_size(att_data_plain) + byte_size(att2_data)
    assert ICouch.Document.attachment_data_size(document2, "anew.dat") === byte_size(att2_data)

    # Set deleted
    deleted_doc = %ICouch.Document{id: "my_id", rev: "my_rev", fields: %{
      "_deleted" => true, "_id" => "my_id", "_rev" => "my_rev"}}
 
    assert ICouch.Document.set_deleted(document) === deleted_doc
    assert ICouch.Document.set_deleted(simple_doc, true) === %{deleted_doc | fields: Map.put(deleted_doc.fields, "k", "v")}

    assert ICouch.Document.set_deleted(ICouch.Document.new()).fields === %{"_deleted" => true}
    assert ICouch.Document.set_deleted(ICouch.Document.new("my_id")).fields === %{"_id" => "my_id", "_deleted" => true}
    assert ICouch.Document.set_deleted(ICouch.Document.new() |> ICouch.Document.set_rev("my_rev")).fields === %{"_rev" => "my_rev", "_deleted" => true}

    # Set id/rev
    simple_doc_wo_id = %ICouch.Document{
      id: nil, rev: "my_rev", attachment_data: %{}, attachment_order: [],
      fields: %{"_rev" => "my_rev", "k" => "v"}}
    simple_doc_wo_rev = %ICouch.Document{
      id: "my_id", rev: nil, attachment_data: %{}, attachment_order: [],
      fields: %{"_id" => "my_id", "k" => "v"}}
    assert ICouch.Document.set_id(simple_doc, nil) === simple_doc_wo_id
    assert ICouch.Document.set_id(simple_doc, "other_id") === %ICouch.Document{
      id: "other_id", rev: "my_rev", attachment_data: %{}, attachment_order: [],
      fields: %{"_id" => "other_id", "_rev" => "my_rev", "k" => "v"}}
    assert ICouch.Document.set_rev(simple_doc, nil) === simple_doc_wo_rev
    assert ICouch.Document.set_rev(simple_doc, "other_rev") === %ICouch.Document{
      id: "my_id", rev: "other_rev", attachment_data: %{}, attachment_order: [],
      fields: %{"_id" => "my_id", "_rev" => "other_rev", "k" => "v"}}

    # Access
    assert simple_doc["_id"] === "my_id"
    assert simple_doc["_rev"] === "my_rev"
    assert simple_doc["k"] === "v"
    assert get_in(document, ["_attachments", "test.txt", "content_type"]) === "text/plain"

    assert ICouch.Document.get(simple_doc, "k") === "v"
    assert ICouch.Document.get(simple_doc, "v", false) === false

    assert put_in(simple_doc["k"], "vv") === %ICouch.Document{
      id: "my_id", rev: "my_rev", attachment_data: %{}, attachment_order: [],
      fields: %{"_id" => "my_id", "_rev" => "my_rev", "k" => "vv"}}
    assert Access.get_and_update(simple_doc, "k", fn (_) -> :pop end) ===
      {"v", %ICouch.Document{id: "my_id", rev: "my_rev", attachment_data: %{},
        attachment_order: [], fields: %{"_id" => "my_id", "_rev" => "my_rev"}}}
    assert_raise RuntimeError, fn -> Access.get_and_update(simple_doc, "k", fn (_) -> :fail end) end

    assert ICouch.Document.pop(simple_doc, "_id") === {"my_id", simple_doc_wo_id}
    assert ICouch.Document.pop(simple_doc, "_rev") === {"my_rev", simple_doc_wo_rev}
    assert ICouch.Document.pop(document2, "_attachments") === {document2["_attachments"], simple_doc}
    assert ICouch.Document.pop(simple_doc, "garbage", false) === {false, simple_doc}

    assert ICouch.Document.put(simple_doc_wo_id, "_id", "my_id") === simple_doc
    assert ICouch.Document.put(simple_doc_wo_rev, "_rev", "my_rev") === simple_doc

    assert ICouch.Document.delete(simple_doc, "_id") === simple_doc_wo_id
    assert ICouch.Document.delete(simple_doc, "_rev") === simple_doc_wo_rev
    assert ICouch.Document.delete(document2, "_attachments") === simple_doc

    # FIXME: Current limitation
    assert_raise ArgumentError, fn -> ICouch.Document.put(simple_doc, "_attachments", %{}) end
  end
end
