#TSQ

[![Build Status](https://travis-ci.org/tadpol/TSQ.svg?branch=master)](https://travis-ci.org/tadpol/TSQ)

An abstraction to help with formatting pickiness and paramater validations.

Built for the [Timeseries storage service](http://beta-docs.exosite.com/murano/services/timeseries/)
in [Murano](https://exosite.com/platform/). 

## Install

- You need to use the [`exosite`](http://beta-docs.exosite.com/murano/exosite-cli/) command line tool to upload modules.
- Edit `Solutionfile.json` to add `tsq.lua`
  ```json
	...
  "modules": {
	  "tsq": "modules/tsq/tsq.lua"
  },
	...
	
	```

## Example

```lua
local qq = TSQ.q():fields('MEAN(temp)'):from('wintd')
qq:where_tag_is('sn', 3):OR_tag_is('sn', 5)
qq:AND_time_ago('1h')
qq:groupby('sn'):groupbytime('15m'):fill('prev'):limit(1)
local out = Timeseries.query{ epoch='ms', q = tostring(qq) }
```


