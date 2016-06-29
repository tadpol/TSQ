TSQ = {}
TSQ.__index = TSQ
TSQ._sel = '*'
TSQ._from = '*'


function TSQ.q()
	local ts = {}
	setmetatable(ts, TSQ)

	return ts
end

function TSQ:from(table)
	self._from = table
	return self
end

function TSQ:limit(value)
	if type(value) ~= "number" then
		-- FIXME do what? return nil? or a special 'failing' object?
		return nil
	end
	self._limit = value
	return self
end

function TSQ:slimit(value)
	if type(value) ~= "number" then
		-- FIXME do what? return nil? or a special 'failing' object?
		return nil
	end
	self._slimit = value
	return self
end

function TSQ:offset(value)
	if type(value) ~= "number" then
		-- FIXME do what? return nil? or a special 'failing' object?
		return nil
	end
	self._offset = value
	return self
end

function TSQ:soffset(value)
	if type(value) ~= "number" then
		-- FIXME do what? return nil? or a special 'failing' object?
		return nil
	end
	self._soffset = value
	return self
end

function TSQ:groupby(tag)
	if type(self._groupby) ~= "table" then self._groupby = {} end
	if type(tag) == "table" then
		-- only copy the array parts
		for i,v in ipairs(tag) do
			self._groupby[#self._groupby + 1] = v
		end
	else
		self._groupby[#self._groupby + 1] = tag
	end
	return self
end

function TSQ:groupbytime(time, offset)
	self._groupbytime = {time, offset}
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
	return self
end

function packageExpr(a, op, b)
-- binary_op = "+" | "-" | "*" | "/" | "AND" | "OR" | "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" .
end

function TSQ:where(...) -- FIXME change to A, op, B; and validate params
	local exr = table.concat(table.pack(...), " ")
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

function TSQ:OR(...) -- FIXME change to A, op, B; and validate params
	local exr = table.concat(table.pack(...), " ")
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

function TSQ.enquote_id(value)
	-- https://docs.influxdata.com/influxdb/v0.13/troubleshooting/frequently_encountered_issues/#single-quoting-and-double-quoting-in-queries
	--[[
	-- Double quote identifiers if they start with a digit, contain characters other
	-- than [A-z,0-9,_], or if they are an InfluxQL keyword. You can double quote
	-- identifiers even if they don’t fall into one of those categories but it isn’t
	-- necessary.
	--]]
	if type(value) == "table" then
		local w = {}
		for i,v in ipairs(value) do
			w[#w + 1] = '"' .. tostring(v) .. '"'
		end
		return table.concat(w, ',')
	else
		return '"' .. tostring(value) .. '"'
	end
end

function TSQ:__tostring()
	local s = 'SELECT '
	s = s .. self.enquote_id(self._sel)

	if type(self._into) == "string" then
		s = s .. ' INTO ' .. tostring(self._into)
	end

	s = s .. ' FROM '
	s = s .. self.enquote_id(self._from)
	
	-- where
	if type(self._where) == "table" then
		s = s .. ' ' .. tostring(self._where)
	end
	
	-- group by
	if type(self._groupby) == "table" then
		local tags = {}
		tags[1] = self.enquote_id(self._groupby)
		if type(self._groupbytime) == "table" and #self._groupbytime > 0 then
			local time = "time(" .. self._groupbytime[1]
			if #self._groupbytime > 1 then
				time = time .. "," .. self._groupbytime[2]
			end
			time = time .. ")"
			tags[#tags + 1] = time
		end
		s = s .. ' GROUP BY ' .. table.concat(tags, ",")
		if self._fill ~= nil then
			s = s .. ' fill(' .. self._fill .. ')'
		end
	end

	-- limit ???XXX Does this require to also have GROUP BY???
	if type(self._limit) == "string" or type(self._limit) == "number" then
		s = s .. ' LIMIT ' .. tostring(self._limit)
	end

	-- slimit ???XXX Does this require to also have GROUP BY???
	if type(self._slimit) == "string" or type(self._slimit) == "number" then
		s = s .. ' SLIMIT ' .. tostring(self._slimit)
	end

	-- offset
	if type(self._offset) == "string" or type(self._offset) == "number" then
		s = s .. ' OFFSET ' .. tostring(self._offset)
	end

	-- soffset
	if type(self._soffset) == "string" or type(self._soffset) == "number" then
		s = s .. ' SOFFSET ' .. tostring(self._soffset)
	end


	return s
end

--[[
-- TODO Create a results iterrator function.
-- reshape results to be a table for each row; keys are column names.
--
--]]

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
