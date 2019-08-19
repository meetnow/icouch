
# ICouch

[![Build Status](https://travis-ci.org/meetnow/icouch.svg)](https://travis-ci.org/meetnow/icouch)
[![Coverage Status](https://coveralls.io/repos/github/meetnow/icouch/badge.svg?branch=master)](https://coveralls.io/github/meetnow/icouch?branch=master)
[![Hex.pm](https://img.shields.io/hexpm/v/icouch.svg)](https://hex.pm/packages/icouch)
[![API Docs](https://img.shields.io/badge/api-docs-yellow.svg?style=flat)](https://hexdocs.pm/icouch/)

## Description

ICouch is a CouchDB client for Elixir using [ibrowse][ibrowse]
for HTTP transfer.

JSON encoding/decoding is provided by [Poison][poison].

ICouch claims to be more "Elixir Style" than other implementations based on
couchbeam and offers a more rigid changes follower tested against vanilla
CouchDB and Cloudant. That said, many calls might look and behave similarly
like couchbeam's since the intent is often the same. However, parameter order
and types will differ in most cases; most notably maps are used instead of
JSON keyword lists.

Another innovation is, that documents are represented as Elixir structs. These
offer a way to conveniently manage attachments in binary form and also implement
the Access behavior and Enumerable protocol for easy handling of a document's
fields.

## Status

This project is currently in beta phase.

ToDo's:
* Implement view streaming
* Implement one-shot changes streaming
* Finish documentation
* Write more tests

## Installation

Add ICouch to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:icouch, "~> 0.5"}]
end
```

## Usage

1. Start a connection
  ```elixir
  # Returns a ICouch.Server struct
  server = ICouch.server_connection("http://username:password@localhost:5984")
  ```

2. Get the instance of DB
  ```elixir
  # Returns ICouch.DB struct
  db = ICouch.assert_db!(server, db_name)
  # db_name is the name of your database

  # You can also do the following for getting the db instance
  ICouch.server_connection("http://username:password@localhost:5984")
    |> ICouch.assert_db!(server, db_name)

  # You can get all the info related to the DB by using
  ICouch.db_info(db)

  # USE THIS WITH CAUTION
  # Destructive operation ahead
  ICouch.delete_db(db)
  ```
3. CRUD operations

### Create
  Simple create operation
  ```elixir
  # It doesn't do anything other than creating a blank document.
  # Most of the time this wouldn't be enough
  doc = ICouch.new()
  {:ok, saved_doc} = ICouch.save_doc(db, doc)
  ```
  Create operation with some data
  ```elixir
  doc_with_data = ICouch.Document.new(
    %{
      release_year: 2013,
      name: "The Hunger Games",
      details: "Movie adaptation of Suzzane Collins books"
    }
  )
  {:ok, saved_doc} = ICouch.save_doc(db, doc)
  # Output would look like this {:ok, %ICouch.Document{}}
  # You can also use the bang variant of save_doc function
  ```

  Create operation with defined ID
  ```elixir
  doc_with_id = ICouch.Document.new("AStarIsBorn")
  saved_doc = ICouch.save_doc!(db, doc_with_id)
  # Output would be %ICouch.Document{}
  ```

  Create with defined ID and REV
  ```elixir
  doc_with_id_and_rev = ICouch.Document.new("AStarIsBorn", "1-AStarIsBorn")
  saved_doc = ICouch.save_doc!(db, doc_with_id_and_rev)
  # Output would be %ICouch.Document{}
  ```

  Create operation without using `ICouch.Document.new/2`
  ```elixir
  ICouch.save_doc!(db, %{"_id" => "MyID", "series_name" => "Man in the high castle"})
  # Output would be %ICouch.Document{}
  # There is a catch in Elixir although we can define maps like %{field: "value"}.
  # But ICouch fails to recognize _id if defined like this.
  # You are better of defining the map like %{"field" => "value"} or at least the _id -'s value
  ```
  Isn't that too many create operations??
  XDXD
  Promise, the next one will be a breather.

### Remove
  Whoops! A user removed some data. We need to remove it from the document too.
  ```elixir
  saved_doc = ICouch.save_doc!(db, %{"title" => "Harry Potter", "has_book" => true, "has_movie_adaptation" => true, "details" => "A mindblowing fiction"})
  key = "details"
  {key_s_value, new_doc} = ICouch.Document.pop(saved_doc, key)
  # Output is tuple {"A mindblowing fiction", %ICouch.Document{}}
  # The document is still not updated. We've to do this manually
  ICouch.save_doc!(db, new_doc)
  # This will return ICouch.Document{}
  # Let us try ICouch.Document.pop/3 once again
  {_, yet_another_doc} = ICouch.Document.pop(saved_doc, key)
  # Notice the difference between the tuples
  ```

### Update
  Received a new data from the user. Time to update the value in the document.
  ```elixir
  # We can update a document in two ways
  # First is update an existing key
  {old_value, new_doc} = ICouch.Document.get_and_update(doc, "title", fn x -> {x, "HarryPotter(Series)"} end)
  saved_rev = ICouch.save_doc!(new_doc)
  # And the second on is adding a completely new key value pair.
  # User just noticed that they accidentally delete the details key-value.
  # Great for us.
  updated_doc = ICouch.Document.put(saved_rev, "details", "A super mindblowing fiction")
  # You will get a new %ICouch.Document{}
  saved_update_doc = ICouch.save_doc!(db, updated_doc)
  ```

### Delete
  Time to delete some stuff
  ```elixir
  ICouch.delete_doc(db, saved_doc)
  # The output would look like {:ok, %{"id" => "an ID", "ok" => true, "rev" => "some UUID"}}
  ```

4. How do I find a particulat data in DB?
  
  Wait up, hoss!!
  
  Before this you need to create a design document. Build `New views` or `New mango index`. This can be done by creating a new document with predefined IDs. You can find the details about doing create operation in **CRUD operations**

  Once you have done so then you can use the following to get started with finding particular keys. And thus the ID of the desired document

  ```elixir
  ICouch.open_view!(db, "myDesignDoc/just-a-view", %{key: "just a key you want to match"})
    |> ICouch.View.fetch!()
  # Return ICouch.View{}
  # From the struct you can get all the matching rows. Save the ID of desired document in the variable *id*
  # Now you can get the document by using the following
  ICouch.open_doc!(db, id)
  # This returns the document with ID equal to *id* in the database *db*
  ```

5. Other tips
  
  - Hopefully you have read points 1 to 4 and have the basic **understanding of how each function is used and their capabilities**.

  - Now you have done the basics and would be interested in using the library in an **Elixir** application or you **Phoenix** framework project. You can start a `GenServer` which manages all the CouchDB related operations.

  - If you find a **feature is not implemented** but you want it so bad that it can't wait. Then you can **do custom requests by using** `ICouch.Server.send_req/4`. Now that you have done this, it'd be great for the community to get the same request added in appropriate Elixir module. Please make the changes, add tests and create a Pull Request.

  - In order to make the examples easy to understand the pipe operator ( `|>` ) was not used. You can use it to make your operations simpler.

6. Something more
  
  Help improve this guide! Found something out of place, or incorrect then just edit it.

  Happy programming!!

## Author

Patrick Schneider / MeetNow! GmbH

## License

Copyright (c) 2017,2018 MeetNow! GmbH

ICouch source code is released under Apache 2 License.

Check the [LICENSE](LICENSE) file for more information.

[ibrowse]: https://hex.pm/packages/ibrowse
[poison]: https://hex.pm/packages/poison
