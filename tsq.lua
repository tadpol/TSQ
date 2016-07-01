--[[
-- An abstraction to the InfluxDB Query language.  Specificly only SELECT statements.
--
-- InfluxDB is quiet picky about placement of spaces and quotes.  So this attempts
-- to programically handle that by abstrating things a bit.  There will be some
-- queries that won't work from here, but most should.
--
-- Because this is abstracted away from string concatenation a bit, it can also do
-- some parameter validation.
--
--]]

------------------------------------------------------------------------------
TSQ = {}
TSQ.__index = TSQ
TSQ._sel = '*'
TSQ._from = '*'

--[[ Quoting Rules:
-- https://docs.influxdata.com/influxdb/v0.13/troubleshooting/frequently_encountered_issues/#single-quoting-and-double-quoting-in-queries
--
-- Single quote string values (for example, tag values) but do not single quote
-- identifiers (database names, retention policy names, user names, measurement
-- names, tag keys, and field keys).
-- 
-- Double quote identifiers if they start with a digit, contain characters
-- other than [A-z,0-9,_], or if they are an InfluxQL keyword. You can double
-- quote identifiers even if they don't fall into one of those categories but
-- it isn't necessary.
-- 
-- Examples: 
-- 	Yes: SELECT bikes_available FROM bikes WHERE station_id='9'
-- 	Yes: SELECT "bikes_available" FROM "bikes" WHERE "station_id"='9'
-- 	Yes: SELECT * from "cr@zy" where "p^e"='2'
-- 	No: SELECT 'bikes_available' FROM 'bikes' WHERE 'station_id'="9"
-- 	No: SELECT * from cr@zy where p^e='2'
--
-- 	Single quote date time strings. InfluxDB returns an error (ERR: invalid operation: time and *influxql.VarRef are not compatible) if you double quote a date time string.
--
-- 	Examples:
-- 	 Yes: SELECT water_level FROM h2o_feet WHERE time > '2015-08-18T23:00:01.232000000Z' AND time < '2015-09-19'
-- 	 No: SELECT water_level FROM h2o_feet WHERE time > "2015-08-18T23:00:01.232000000Z" AND time < "2015-09-19"
--
--]]

function TSQ.q()
	local ts = {}
	setmetatable(ts, TSQ)

	return ts
end

function TSQ:fields(...)
	if type(self._sel) ~= "table" then
		local ft = {}
		local ftm = {}
		ftm.__tostring = function(v)
			return table.concat(v, ',')
		end
		setmetatable(ft, ftm)
		self._sel = ft
	end
	local tbl = {}
	if select('#', ...) == 1 then
		local v = select(1, ...)
		if type(v) == "table" then
			tbl = v
		else
			tbl[1] = tostring(v)
		end
	else
		tbl = table.pack(...)
	end
	for i,v in ipairs(tbl) do -- FIXME Needs to be smarter since can be expr.
		-- func(field)
		-- AS name
		local vs = tostring(v)
		fn, fl = string.match(vs, "^(%a+)%((.*)%)$") -- might be too simplistic.
		if fn ~= nil and fl ~= nil then
			-- this is a function call on a field. 
			-- Quote the field only
			vs = fn .. '("' .. fl .. '")'
		else
			vs =  '"' .. vs .. '"'
		end
		self._sel[#self._sel + 1] = vs
	end
	return self
end

function TSQ:from(...)
	if type(self._from) ~= "table" then
		local ft = {}
		local ftm = {}
		ftm.__tostring = function(v)
			return table.concat(v, ',')
		end
		setmetatable(ft, ftm)
		self._from = ft
	end
	local tbl = {}
	if select('#', ...) == 1 then
		local v = select(1, ...)
		if type(v) == "table" then
			tbl = v
		else
			tbl[1] = tostring(v)
		end
	else
		tbl = table.pack(...)
	end
	for i,v in ipairs(tbl) do
		-- if quotes or / already surround string, don't add quotes.
		local vs = tostring(v)
		if (vs:sub(1,1) == '"' and vs:sub(-1,-1) == '"') or
			(vs:sub(1,1) == '/' and vs:sub(-1,-1) == '/') then
		else
			vs = string.format('%q', vs)
		end
		self._from[#self._from + 1] = vs
	end
	return self
end

function TSQ:limit(value)
	if type(value) ~= "number" then
		error("limit ("..value..") is not a number")
	end
	self._limit = value
	return self
end

function TSQ:slimit(value)
	if type(value) ~= "number" then
		error("slimit ("..value..") is not a number")
	end
	self._slimit = value
	return self
end

function TSQ:offset(value)
	if type(value) ~= "number" then
		error("offset ("..value..") is not a number")
	end
	self._offset = value
	return self
end

function TSQ:soffset(value)
	if type(value) ~= "number" then
		error("soffset ("..value..") is not a number")
	end
	self._soffset = value
	return self
end

function TSQ:groupby(...)
	if type(self._groupby) ~= "table" then
		local ft = {}
		local ftm = {}
		ftm.__tostring = function(v)
			return table.concat(v, ',')
		end
		setmetatable(ft, ftm)
		self._groupby = ft
	end
	local tbl = {}
	if select('#', ...) == 1 then
		local v = select(1, ...)
		if type(v) == "table" then
			tbl = v
		else
			tbl[1] = tostring(v)
		end
	else
		tbl = table.pack(...)
	end
	for i,v in ipairs(tbl) do
		local vs = string.format('%q', tostring(v))
		self._groupby[#self._groupby + 1] = vs
	end
	return self
end

function TSQ.is_a_duration(value)
	local vs = tostring(value)
	local dig, units = string.match(vs, "^(%d+)([usmhdw]*)$")
	if dig == nil then
		return false
	end
	local allowed_units = {'u', 'ms', 's', 'm', 'h', 'd', 'w'}
	for i,v in ipairs(allowed_units) do
		if v == units then
			return true
		end
	end
	return false
end

function TSQ:groupbytime(time, offset)
	if not TSQ.is_a_duration(time) then
		return nil
	end
	if offset ~= nil and not TSQ.is_a_duration(offset) then
		return nil
	end
	local gbt = {time, offset}
	local gbtmeta = {}
	gbtmeta.__tostring = function(v)
		if #v <= 0 then return "" end
		local time = "time( " .. tostring(v[1])
		if #v > 1 then
			time = time .. ", " .. tostring(v[2])
		end
		return time .. " )"
	end
	setmetatable(gbt, gbtmeta)
	self._groupbytime = gbt
	return self
end

function TSQ:fill(opt)
	if type(opt) == "number" then
		self._fill = opt
	elseif type(opt) == "string" then
		if opt == "null" or opt == "none" or opt == "previous" then
			self._fill = opt
		elseif opt == "prev" then
			self._fill = "previous"
		end
	else
		return nil
	end
	-- Added a fill, make sure there is a groupby
	if self._groupby == nil then
		self:groupby('*')
	end
	return self
end

function TSQ.is_an_op(op)
	--local binary_op = {"+", "-", "*", "/", "AND", "OR", "=", "!=", "<>", "<", "<=", ">", ">="}
	local binary_op = {"=", "!=", "<>", "<", "<=", ">", ">=", "=~", "!~"}
	for i,v in ipairs(binary_op) do
		if v == op then
			return true
		end
	end
	return false
end

function TSQ.packageExpr(a, op, b)
	if a == nil or b == nil or op == nil then
		error("no nils allowed (" .. a .. ', ' .. op .. ', ' .. b ..")")
	end
	if not TSQ.is_an_op(op) then
		error("operation (" .. op .. ") is not valid")
	end
	
	-- FIXME quoting.

	return tostring(a) .. ' ' .. op .. ' ' .. tostring(b)
end

function TSQ:where(a, op, b)
	local exr = self.packageExpr(a, op, b)
	if type(self._where) ~= "table" then
		local andtbl = {}
		local andmeta = {}
		andmeta.__tostring = function(v)
			local w = {}
			for i,v in ipairs(v) do
				w[#w + 1] = tostring(v)
			end
			return table.concat(w, ' AND ')
		end
		setmetatable(andtbl, andmeta)
		self._where = andtbl
	end
	self._where[#self._where + 1] = exr
	-- ANDs push to the end of where.
	return self
end
TSQ.AND = TSQ.where

function TSQ:OR(a, op, b)
	local exr = self.packageExpr(a, op, b)
	local ortbl = {}
	if type(self._where[#self._where]) ~= "table" then
		-- convert last item into a table
		local ormeta = {}
		ormeta.__tostring = function(v)
			local w = {}
			for i,v in ipairs(v) do
				w[#w + 1] = tostring(v)
			end
			return '( ' .. table.concat(w, ' OR ') .. ' )'
		end
		setmetatable(ortbl, ormeta)

		ortbl[1] = self._where[#self._where]
		self._where[#self._where] = ortbl
	else
		ortbl = self._where[#self._where]
	end
	ortbl[#ortbl + 1] = exr
	-- ORs push onto an array that is at the end of self.where

	return self
end

--[[
-- q:where_time...
--]]

-- Specific where clauses for tags.
for i,v in ipairs{'where','AND','OR'} do
	TSQ[v .. "_tag_is"] = function(me, tag, value)
		return me[v](me, tag, '=', "'" .. tostring(value) .. "'")
	end
	TSQ[v .. "_tag_isnot"] = function(me, tag, value)
		return me[v](me, tag, '!=', "'" .. tostring(value) .. "'")
	end
	TSQ[v .. "_tag_matches"] = function(me, tag, value)
		return me[v](me, tag, '=~', "/" .. tostring(value) .. "/")
	end
	TSQ[v .. "_tag_not_matching"] = function(me, tag, value)
		return me[v](me, tag, '!~', "/" .. tostring(value) .. "/")
	end
end

-- Specific where clauses for fields.
for i,v in ipairs{'where','AND','OR'} do
	TSQ[v .. "_field_is"] = function(me, field, value)
		return me[v](me, field, '=', tostring(value))
	end
	TSQ[v .. "_field_isnot"] = function(me, field, value)
		return me[v](me, field, '!=', tostring(value))
	end
	TSQ[v .. "_field_greater"] = function(me, field, value)
		return me[v](me, field, '>', tostring(value))
	end
	TSQ[v .. "_field_less"] = function(me, field, value)
		return me[v](me, field, '<', tostring(value))
	end
end

function TSQ:__tostring()
	local s = 'SELECT '
	s = s .. tostring(self._sel)

	if type(self._into) == "string" then
		s = s .. ' INTO ' .. tostring(self._into)
	end

	s = s .. ' FROM '.. tostring(self._from)
	
	if type(self._where) == "table" then
		s = s .. ' WHERE ' .. tostring(self._where)
	end
	
	if type(self._groupby) == "table" or type(self._groupbytime) == "table" then
		local tags = {}
		if type(self._groupby) == "table" then
			tags[#tags + 1] = tostring(self._groupby)
		end
		if type(self._groupbytime) == "table" and #self._groupbytime > 0 then
			tags[#tags + 1] = tostring(self._groupbytime)
		end
		s = s .. ' GROUP BY ' .. table.concat(tags, ",")
		if self._fill ~= nil then
			s = s .. ' fill(' .. self._fill .. ')'
		end
	end

	if type(self._orderby) == "table" then
		s = s .. ' ORDER BY ' .. tostring(self._orderby)
	end

	if type(self._limit) == "string" or type(self._limit) == "number" then
		s = s .. ' LIMIT ' .. tostring(self._limit)
	end

	if type(self._offset) == "string" or type(self._offset) == "number" then
		s = s .. ' OFFSET ' .. tostring(self._offset)
	end

	if type(self._slimit) == "string" or type(self._slimit) == "number" then
		s = s .. ' SLIMIT ' .. tostring(self._slimit)
	end

	if type(self._soffset) == "string" or type(self._soffset) == "number" then
		s = s .. ' SOFFSET ' .. tostring(self._soffset)
	end

	return s
end

if _VERSION == "Lua 5.1" then
	table.unpack = unpack
end
------------------------------------------------------------------------------
---
-- Return the next values in the series returned from Timeseries.
-- This works much like Lua's next() function.
-- If there are multiple series, then the next for each is returned
--
-- This returns the next index or nil if at the end.
-- Following are tables for each series where the keys are the column names and the
-- values are from the current row.
function TSQ.next_in_series(serieses, idx)
	if idx == nil then idx = 1 else idx = idx + 1 end
	local result = {}
	local has_values = 0
	for i,series in ipairs(serieses) do
		local value = series.values[idx]
		local ret = {}
		if value ~= nil then
			has_values = has_values + 1
			for i,v in ipairs(series.columns) do
				ret[v] = value[i]
			end
		end
		result[#result + 1] = ret
	end

	-- if all of the series are empty, then stop.
	if has_values == 0 then
		return nil
	else
		return idx, table.unpack(result)
	end
end

function TSQ.series_ipairs(series)
	return TSQ.next_in_series, series, nil
end

------------------------------------------------------------------------------
---
-- Dump a table as a string; recursively
-- \returns string
function dump_table(o)
    if type(o) == 'table' then
        local s = '{ '
        for k,v in pairs(o) do
            if type(k) ~= 'number' then k = '"'..k..'"' end
            s = s .. '['..k..'] = ' .. dump_table(v) .. ','
        end
        return s .. '} '
    else
        return tostring(o)
    end
end

-- vim: set ai sw=4 ts=4 :
