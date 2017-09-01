
# Created by Patrick Schneider on 03.06.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule ICouchTest do
  use ExUnit.Case, async: false
  use ExVCR.Mock

  setup_all do
    ExVCR.Config.cassette_library_dir("fixture/vcr_cassettes")
    :ok
  end

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

  test "server connection" do
    # Connect default
    assert ICouch.server_connection() === %ICouch.Server{
      direct: nil, ib_options: [], timeout: nil, uri: %URI{
        authority: "127.0.0.1:5984", fragment: nil, host: "127.0.0.1",
        path: nil, port: 5984, query: nil, scheme: "http", userinfo: nil}}

    # Connect specific with auth
    assert ICouch.server_connection("http://admin:admin@192.168.99.100:8400/") ===
      %ICouch.Server{direct: nil, ib_options: [basic_auth: {'admin', 'admin'}],
        timeout: nil, uri: %URI{authority: "192.168.99.100", fragment: nil,
          host: "192.168.99.100", path: "/", port: 8400, query: nil,
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

  test "server requests" do
    s = ICouch.server_connection("http://192.168.99.100:8400/")

    use_cassette "server_requests", match_requests_on: [:query] do
      :ibrowse.start()

      # Get server info
      assert ICouch.server_info(s) === {:ok, %{"couchdb" => "Welcome",
        "uuid" => "c75f41109dc0ec3a0393a46fc2f80a91", "vendor" => %{
          "name" => "The Apache Software Foundation", "version" => "1.6.1"},
        "version" => "1.6.1"}}

      # Get all databases
      assert ICouch.all_dbs(s) === {:ok, ["_replicator", "_users", "test_db"]}

      # Get a single uuid
      assert ICouch.get_uuid!(s) === "0fd31ad981e909e2781ecb9d090411d4"

      # Get a number of uuids
      assert ICouch.get_uuids!(s, 3) === ["0fd31ad981e909e2781ecb9d090414f5",
        "0fd31ad981e909e2781ecb9d0904204d", "0fd31ad981e909e2781ecb9d090425be"]
    end
  end

  test "database management" do
    s = ICouch.server_connection("http://192.168.99.100:8400/")
    sa = ICouch.server_connection("http://admin:admin@192.168.99.100:8400/")

    use_cassette "database_management" do
      :ibrowse.start()

      # Try to open a non-existent database
      assert_raise ICouch.RequestError, "Not Found", fn -> ICouch.open_db!(s, "nonexistent") end

      # Try to create a database without authorization
      assert_raise ICouch.RequestError, "Unauthorized", fn -> ICouch.assert_db!(s, "fail_new") end

      # Create a database
      d = ICouch.assert_db!(sa, "test_new")
      assert d === %ICouch.DB{name: "test_new", server: sa}

      # Get database info
      assert ICouch.db_info(d) === {:ok, %{"committed_update_seq" => 0,
        "compact_running" => false, "data_size" => 0, "db_name" => "test_new",
        "disk_format_version" => 6, "disk_size" => 79, "doc_count" => 0,
        "doc_del_count" => 0, "instance_start_time" => "1502904690851994",
        "purge_seq" => 0, "update_seq" => 0}}

      # Delete a database
      assert ICouch.delete_db(d) === :ok

      # Try to delete a non-existent database
      assert ICouch.delete_db(sa, "nonexistent") === {:error, :not_found}

      # Try to create an existing database
      assert_raise ICouch.RequestError, "Precondition Failed", fn -> ICouch.create_db!(sa, "test_db") end
    end
  end

  test "document handling" do
    s = ICouch.server_connection("http://192.168.99.100:8400/")

    att_doc = %ICouch.Document{attachment_data: %{},
      attachment_order: ["small-jpeg.jpg"], fields: %{
        "_attachments" => %{"small-jpeg.jpg" => %{
          "content_type" => "image/jpeg",
          "digest" => "md5-VY+mp2HtUEbf51mWfJQi0g==", "length" => 125,
          "revpos" => 6, "stub" => true}},
        "_id" => "att_doc", "_rev" => "7-2e8de0edb67e630ce9fa90cc5fe1062d",
        "key" => "le_key", "value" => "la_value"},
      id: "att_doc", rev: "7-2e8de0edb67e630ce9fa90cc5fe1062d"}

    att_data = Base.decode64!("/9j/4AAQSkZJRgABAQEASABIAAD/2wBDAAMCAgICAgMCAgIDAwMDBAYEBAQEBAgGBgUGCQgKCgkICQkKDA8MCgsOCwkJDRENDg8QEBEQCgwSExIQEw8QEBD/yQALCAABAAEBAREA/8wABgAQEAX/2gAIAQEAAD8A0s8g/9k=")

    att_doc_whad = ICouch.Document.put_attachment_data(att_doc, "small-jpeg.jpg", att_data)

    new_doc = %ICouch.Document{attachment_data: %{
        "test" => "This is a very simple text file."},
      attachment_order: ["test"], fields: %{"_attachments" => %{
          "test" => %{"content_type" => "application/octet-stream", "length" => 32,
            "stub" => true}},
        "_id" => "new_doc8", "key" => "the_key", "value" => "the_value"},
      id: "new_doc8", rev: nil}

    saved_doc = ICouch.Document.set_rev(new_doc, "1-12c0ccf8993ea47d1a7893cb0b8dae3e")

    saved_doc_whai = ICouch.Document.put_attachment_info(saved_doc, "test", %{
      "content_type" => "application/octet-stream", "length" => 32, "stub" => true,
      "digest" => "md5-xhhZ0oUF8fnIYvXlVxS1PQ==", "revpos" => 1})

    use_cassette "document_handling", match_requests_on: [:query, :headers] do
      :ibrowse.start()
      d = ICouch.open_db!(s, "test_db")

      # Test for non-existent document
      assert ICouch.doc_exists?(d, "nonexistent") === false
      assert ICouch.get_doc_rev(d, "nonexistent") === {:error, :not_found}
      assert_raise ICouch.RequestError, "Not Found", fn -> ICouch.open_doc!(d, "nonexistent") end

      # Test for existing document
      assert ICouch.doc_exists?(d, "att_doc") === true
      assert ICouch.get_doc_rev(d, "att_doc") === {:ok, "7-2e8de0edb67e630ce9fa90cc5fe1062d"}

      # Open document
      assert ICouch.open_doc!(d, "att_doc") === att_doc

      # Open document with attachments (using Base64)
      assert ICouch.open_doc!(d, "att_doc", attachments: true, multipart: false) === att_doc_whad

      # Create document
      assert ICouch.save_doc!(d, new_doc, multipart: "UnitTestBoundary") === saved_doc

      # Re-open document (using Multipart)
      assert ICouch.open_doc!(d, "new_doc8", attachments: true) === saved_doc_whai

      # Duplicate document
      assert {:ok, dup_response} = ICouch.dup_doc(d, saved_doc)

      # Delete documents
      assert {:ok, %{"id" => "new_doc8", "ok" => true}} = ICouch.delete_doc(d, saved_doc)
      assert {:ok, %{"ok" => true}} = ICouch.delete_doc(d, dup_response["id"], rev: dup_response["rev"])
    end
  end
end
