
describe("Query generation", function()
	require "tsq"

	it("provide the most basic query when called without extensions", function()
		local s = tostring(TSQ.q())
		assert.are.equal("SELECT * FROM *", s)
	end)

	it("checks the ways that select fields can be specificed", function()
		local s
		s = tostring(TSQ.q():fields('af'))
		assert.are.equal([[SELECT "af" FROM *]], s)

		s = tostring(TSQ.q():fields('af', 'gf', 'gh'))
		assert.are.equal([[SELECT "af","gf","gh" FROM *]], s)

		s = tostring(TSQ.q():fields({'af', 'gf', 'gh'}))
		assert.are.equal([[SELECT "af","gf","gh" FROM *]], s)

		s = tostring(TSQ.q():fields{'af', 'gf', 'gh'})
		assert.are.equal([[SELECT "af","gf","gh" FROM *]], s)
	end)

	it("checks that functions on fields", function()
		local s
		s = tostring(TSQ.q():fields('mean(af)'))
		assert.are.equal(s, [[SELECT mean("af") FROM *]])
	end)




	it("a complex one.", function()
		local s
		s = TSQ.q():fields('MEAN(temp)'):from('wintd'):where('sn', '=', "'3'"):OR('sn', '=', "'5'"):AND('time','>','now() - 1h'):groupby('sn'):groupbytime('15m'):fill('prev'):limit(1)
		r = [[SELECT MEAN("temp") FROM "wintd" WHERE ( sn = '3' OR sn = '5' ) AND time > now() - 1h GROUP BY "sn",time( 15m ) fill(previous) LIMIT 1]]
		assert.are.equal(r, tostring(s))
	end)
end)

-- vim: set ai sw=4 ts=4 :
