#TSQ

An abstraction to help with formatting pickiness and paramater validations.

Built for the [Timeseries storage service][TSS] in [Murano][]. 

## Install

- You need to use the [`exosite`][ect] command line tool to upload modules.
- Edit `Solutionfile.json` to add `tsq.lua`
  ```
	...
  "modules": {
	  "tsq": "modules/tsq/tsq.lua"
  },
	...
	
	```

## Example

```
local qq = TSQ.q():fields('MEAN(temp)'):from('wintd')
qq:where_tag_is('sn', 3):OR_tag_is('sn', 5)
qq:AND_time_ago('1h')
qq:groupby('sn'):groupbytime('15m'):fill('prev'):limit(1)
local out = Timeseries.query{ epoch='ms', q = tostring(qq) }
```



[Murano]: https://exosite.com/platform/ 
[TSS]: http://beta-docs.exosite.com/murano/services/timeseries/
[ect]: http://beta-docs.exosite.com/murano/exosite-cli/ 
