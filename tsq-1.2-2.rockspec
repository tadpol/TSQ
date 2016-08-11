package = "TSQ"
version = "1.2-2"
source = {
   url = "https://github.com/tadpol/TSQ.git",
   tag = "v1.2-2"
}
description = {
   summary = "Murano Timeseries (InfluxDB) Lua abstraction",
   detailed = [[
An abstraction to help with formatting pickiness and paramater validations.

Built for the Timeseries storage service in Murano.
   ]],
   homepage = "https://github.com/tadpol/TSQ",
   license = "MIT"
}
dependencies = {
   "lua >= 5.1"
}
build = {
	type = "builtin",
	modules = {
		tsq = "tsq.lua",
		tsw = "tsw.lua",
	}
}
