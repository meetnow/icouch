
# ICouch

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
* Implement view handling
* Implement one-shot changes handling
* Implement replication mix task
* Finish documentation
* Write tests

## Installation

Add ICouch to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:icouch, "~> 0.1"}]
end
```

## Usage

(todo)

## Author

Patrick Schneider / MeetNow! GmbH

## License

Copyright (c) 2017 MeetNow! GmbH

ICouch source code is released under Apache 2 License.

Check the [LICENSE](LICENSE) file for more information.

[ibrowse]: https://hex.pm/packages/ibrowse
[poison]: https://hex.pm/packages/poison
