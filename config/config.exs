
# Created by Patrick Schneider on 03.06.2017.
# Copyright (c) 2017 MeetNow! GmbH

use Mix.Config

if Mix.env == :test,
  do: config :logger, backends: [],
  else: config :logger, console: [format: "$time [$level] $levelpad$metadata$message\n", metadata: [:module, :via]]
