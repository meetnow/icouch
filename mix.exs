
# Created by Patrick Schneider on 03.06.2017.
# Copyright (c) 2017 MeetNow! GmbH

defmodule ICouch.Mixfile do
  use Mix.Project

  def project do
    [app: :icouch,
     name: "ICouch",
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     source_url: "https://github.com/meetnow/icouch",
     homepage_url: "https://github.com/meetnow/icouch",
     docs: [main: "readme", extras: ["README.md"]],
     description: description(),
     package: package(),
     deps: deps()]
  end

  defp description do
    """
    A CouchDB client for Elixir using ibrowse for HTTP transfer
    """
  end

  def application do
    [extra_applications: [:logger, :ssl]]
  end

  defp deps do
    [{:ibrowse, "~> 4.4"},
     {:poison, "~> 3.1"},
     {:ex_doc, "~> 0.16.1", only: :dev}]
  end

  defp package do
    [maintainers: ["Patrick Schneider <patrick.schneider@meetnow.eu>"],
     licenses: ["Apache 2.0"],
     links: %{"GitHub" => "https://github.com/meetnow/icouch"}]
  end
end
