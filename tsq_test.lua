
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
		assert.are.equal([[SELECT mean("af") FROM *]], s)
	end)

	it("Check the ways metrics can be from", function()
		local q
		q = TSQ.q():from('af')
		assert.are.equal([[SELECT * FROM "af"]], tostring(q))

		q = TSQ.q():from('af', 'bg', 'ce')
		assert.are.equal([[SELECT * FROM "af","bg","ce"]], tostring(q))

		q = TSQ.q():from({'af', 'bg', 'ce'})
		assert.are.equal([[SELECT * FROM "af","bg","ce"]], tostring(q))

		q = TSQ.q():from{'af', 'bg', 'ce'}
		assert.are.equal([[SELECT * FROM "af","bg","ce"]], tostring(q))

		q = TSQ.q():from('"af"')
		assert.are.equal([[SELECT * FROM "af"]], tostring(q))

		q = TSQ.q():from('/af/')
		assert.are.equal([[SELECT * FROM /af/]], tostring(q))

	end)

	it("can have a limit and/or offset", function()
		local q
		q = TSQ.q():limit(10)
		assert.are.equal([[SELECT * FROM * LIMIT 10]], tostring(q))

		q = TSQ.q():offset(10)
		assert.are.equal([[SELECT * FROM * OFFSET 10]], tostring(q))

		q = TSQ.q():limit(10):offset(10)
		assert.are.equal([[SELECT * FROM * LIMIT 10 OFFSET 10]], tostring(q))

		q = TSQ.q():offset(10):limit(10)
		assert.are.equal([[SELECT * FROM * LIMIT 10 OFFSET 10]], tostring(q))

	end)

	it("can have a slimit and/or soffset", function()
		local q
		q = TSQ.q():slimit(10)
		assert.are.equal([[SELECT * FROM * SLIMIT 10]], tostring(q))

		q = TSQ.q():soffset(10)
		assert.are.equal([[SELECT * FROM * SOFFSET 10]], tostring(q))

		q = TSQ.q():slimit(10):soffset(10)
		assert.are.equal([[SELECT * FROM * SLIMIT 10 SOFFSET 10]], tostring(q))

		q = TSQ.q():soffset(10):slimit(10)
		assert.are.equal([[SELECT * FROM * SLIMIT 10 SOFFSET 10]], tostring(q))

	end)

	it("checks the ways that groupby can be specificed", function()
		local s
		s = tostring(TSQ.q():groupby('af'))
		assert.are.equal([[SELECT * FROM * GROUP BY "af"]], s)

		s = tostring(TSQ.q():groupby('af', 'gf', 'gh'))
		assert.are.equal([[SELECT * FROM * GROUP BY "af","gf","gh"]], s)

		s = tostring(TSQ.q():groupby({'af', 'gf', 'gh'}))
		assert.are.equal([[SELECT * FROM * GROUP BY "af","gf","gh"]], s)

		s = tostring(TSQ.q():groupby{'af', 'gf', 'gh'})
		assert.are.equal([[SELECT * FROM * GROUP BY "af","gf","gh"]], s)
	end)

	it("checks the ways that groupbytime can be specificed", function()
		local s
		s = tostring(TSQ.q():groupbytime('13h'))
		assert.are.equal([[SELECT * FROM * GROUP BY time( 13h )]], s)
	end)

	--------------------------------------------------------------
	it("parts can be added across multiple lines.", function()
		local q = TSQ.q()
		q:from("bob")
		q:fields("a")
		s = tostring(q)
		assert.are.equal([[SELECT "a" FROM "bob"]], s)
	end)

	it("a complex one for a real query", function()
		local s
		s = TSQ.q():fields('MEAN(temp)'):from('wintd'):where('sn', '=', "'3'"):OR('sn', '=', "'5'"):AND('time','>','now() - 1h'):groupby('sn'):groupbytime('15m'):fill('prev'):limit(1)
		r = [[SELECT MEAN("temp") FROM "wintd" WHERE ( sn = '3' OR sn = '5' ) AND time > now() - 1h GROUP BY "sn",time( 15m ) fill(previous) LIMIT 1]]
		assert.are.equal(r, tostring(s))
	end)

end)

-- vim: set ai sw=4 ts=4 :
