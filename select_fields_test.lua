describe("Select Fields with Functions", function()
	require "tsq"

	it("Can create just a field", function()
		local s
		s = tostring(TSF.new('time'))
		assert.are.equal([["time"]], s)
	end)

	it("Can create all the single functions", function()
		local s
		s = tostring(TSF.new('time'):count())
		assert.are.equal([[COUNT("time")]], s)

		s = tostring(TSF.new('time'):distinct())
		assert.are.equal([[DISTINCT("time")]], s)

		s = tostring(TSF.new('time'):mean())
		assert.are.equal([[MEAN("time")]], s)

		s = tostring(TSF.new('time'):median())
		assert.are.equal([[MEDIAN("time")]], s)

		s = tostring(TSF.new('time'):spread())
		assert.are.equal([[SPREAD("time")]], s)

		s = tostring(TSF.new('time'):sum())
		assert.are.equal([[SUM("time")]], s)

		s = tostring(TSF.new('time'):first())
		assert.are.equal([[FIRST("time")]], s)

		s = tostring(TSF.new('time'):last())
		assert.are.equal([[LAST("time")]], s)

		s = tostring(TSF.new('time'):max())
		assert.are.equal([[MAX("time")]], s)

		s = tostring(TSF.new('time'):min())
		assert.are.equal([[MIN("time")]], s)

		s = tostring(TSF.new('time'):difference())
		assert.are.equal([[DIFFERENCE("time")]], s)

		s = tostring(TSF.new('time'):stddev())
		assert.are.equal([[STDDEV("time")]], s)

	end)

	it("Can create all the number param functions", function()
		local s
		s = tostring(TSF.new('time'):bottom(4))
		assert.are.equal([[BOTTOM("time", 4)]], s)

		s = tostring(TSF.new('time'):percentile(4))
		assert.are.equal([[PERCENTILE("time", 4)]], s)

		s = tostring(TSF.new('time'):top(4))
		assert.are.equal([[TOP("time", 4)]], s)

		s = tostring(TSF.new('time'):moving_average(4))
		assert.are.equal([[MOVING_AVERAGE("time", 4)]], s)

	end)

	it("throws errors if param is not a number", function()
		assert.has_error(function() TSF.new('time'):bottom('k') end)
		assert.has_error(function() TSF.new('time'):bottom(true) end)
		assert.has_error(function() TSF.new('time'):bottom({1,2,3}) end)
	end)

	it("Can create all the duration param functions", function()
		local s
		s = tostring(TSF.new('time'):derivative('4s'))
		assert.are.equal([[DERIVATIVE("time", 4s)]], s)

		s = tostring(TSF.new('time'):elapsed('3m'))
		assert.are.equal([[ELAPSED("time", 3m)]], s)

		s = tostring(TSF.new('time'):non_negative_derivative('12d'))
		assert.are.equal([[NON_NEGATIVE_DERIVATIVE("time", 12d)]], s)
	end)

	it("throws errors if param is not a duration", function()
		assert.has_error(function() TSF.new('time'):derivative('k') end)
		assert.has_error(function() TSF.new('time'):derivative(true) end)
		assert.has_error(function() TSF.new('time'):derivative(6) end)
		assert.has_error(function() TSF.new('time'):derivative({1,2,3}) end)
	end)

	it("can specify a name with as", function()
		local s
		s = tostring(TSF.new('time'):as("forever"))
		assert.are.equal([["time" AS "forever"]], s)

		s = tostring(TSF.new('time'):count():as("forever"))
		assert.are.equal([[COUNT("time") AS "forever"]], s)

		s = tostring(TSF.new('time'):as("forever"):count())
		assert.are.equal([[COUNT("time") AS "forever"]], s)

		s = tostring(TSF.new('time'):top(3):as("forever"))
		assert.are.equal([[TOP("time", 3) AS "forever"]], s)

	end)
end)
-- vim: set ai sw=2 ts=2 :
