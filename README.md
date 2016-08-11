#TSQ

[![Build Status](https://travis-ci.org/tadpol/TSQ.svg?branch=master)](https://travis-ci.org/tadpol/TSQ)
![rock version](https://img.shields.io/badge/rock%20version-1.2--1-brightgreen.svg)


An abstraction to help with formatting pickiness and paramater validations.

Built for the [Timeseries storage service](http://beta-docs.exosite.com/murano/services/timeseries/)
in [Murano](https://exosite.com/platform/). 

## Install

You need to use either [`exosite`](http://docs.exosite.com/murano/exosite-cli/) or
[MrMurano]()


### Using MrMurano
Clone or add as a submodule into the modules directory.
```
cd modules
git clone https://github.com/tadpol/TSQ 
```
OR
```
cd modules
git submodule add https://github.com/tadpol/TSQ
```

### Using exosite
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
local qq = TSQ.q():fields(TSF.new('temp'):mean())
qq:from('wintd')
qq:where_tag_is('sn', 3):OR_tag_is('sn', 5)
qq:AND_time_ago('1h')
qq:groupby('sn'):groupbytime('15m'):fill('prev'):limit(1)
local out = Timeseries.query{ epoch='ms', q = tostring(qq) }
```


