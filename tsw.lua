--[[
--  Some abstraction for writing to InfluxDB
--]]

TSW = {}

function TSW.table_to_idb(tbl)
	if tbl == nil then
		return ""
	end
	local building = {}
	for k,v in pairs(tbl) do
		building[#building + 1] = tostring(k) .."=" .. tostring(v)
	end
	return table.concat(building, ",")
end

---
-- Build a InfluxDB write command from lua tables.
function TSW.write(metric, tags, fields, timestamp)
	local s = tostring(metric)
	s = s .. ","
	s = s .. TSW.table_to_idb(tags)
	s = s .. " "
	s = s .. TSW.table_to_idb(fields)
	if timestamp ~= nil then
		s = s .. " " .. timestamp
	end
	return s
end


-- vim: set ai sw=4 ts=4 :
