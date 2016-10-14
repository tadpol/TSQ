-- luacheck: globals describe it (busted globals)

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

		local q
		q = TSQ.q():fields('af')
		q:fields('gf')
		q:fields('gh')
		s = tostring(q)
		assert.are.equal([[SELECT "af","gf","gh" FROM *]], s)
	end)

	it("checks that functions on fields", function()
		local s
		s = tostring(TSQ.q():fields('mean(af)'))
		assert.are.equal([[SELECT mean("af") FROM *]], s)
	end)

	it("checks fields from TSF", function()
		local s, f, q
		f = TSF.new('time')
		q = TSQ.q():fields(f)
		s = tostring(q)
		assert.are.equal([[SELECT "time" FROM *]], s)

		f = TSF.new('time'):count()
		q = TSQ.q():fields(f)
		s = tostring(q)
		assert.are.equal([[SELECT COUNT("time") FROM *]], s)

		f = TSF.new('time'):top(5)
		q = TSQ.q():fields(f)
		s = tostring(q)
		assert.are.equal([[SELECT TOP("time", 5) FROM *]], s)

		f = TSF.new('time'):as('bob')
		q = TSQ.q():fields(f)
		s = tostring(q)
		assert.are.equal([[SELECT "time" AS "bob" FROM *]], s)

	end)
	it("checks multiple fields from TSF", function()
		local s, q
		q = TSQ.q():fields(TSF.new('time'), TSF.new('value'))
		s = tostring(q)
		assert.are.equal([[SELECT "time","value" FROM *]], s)

		q = TSQ.q():fields(TSF.new('time'):min(), TSF.new('time'):max())
		s = tostring(q)
		assert.are.equal([[SELECT MIN("time"),MAX("time") FROM *]], s)

		q = TSQ.q():fields(TSF.new('time'):as('fore'), TSF.new('value'):as('v'))
		s = tostring(q)
		assert.are.equal([[SELECT "time" AS "fore","value" AS "v" FROM *]], s)

	end)
	it("checks fields mixed with TSF", function()
		local s, q
		q = TSQ.q():fields(TSF.new('time'), 'value')
		s = tostring(q)
		assert.are.equal([[SELECT "time","value" FROM *]], s)

		q = TSQ.q():fields(TSF.new('time'):as('g'), 'value')
		s = tostring(q)
		assert.are.equal([[SELECT "time" AS "g","value" FROM *]], s)

		q = TSQ.q():fields('time', TSF.new('value'):mean())
		s = tostring(q)
		assert.are.equal([[SELECT "time",MEAN("value") FROM *]], s)

	end)

	it("checks for nested functions", function()
		local s
		s = tostring(TSQ.q():fields('DERIVATIVE(mean(af))'))
		assert.are.equal([[SELECT DERIVATIVE(mean("af")) FROM *]], s)

		s = tostring(TSQ.q():fields('DERIVATIVE(mean(af),6m)'))
		assert.are.equal([[SELECT DERIVATIVE(mean("af"),6m) FROM *]], s)
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

		s = tostring(TSQ.q():groupbytime('13h', '2d'))
		assert.are.equal([[SELECT * FROM * GROUP BY time( 13h, 2d )]], s)

	end)

	it("checks where tag clauses", function()
		local q
		q = TSQ.q():where_tag_is('sn', 5)
		assert.are.equal([[SELECT * FROM * WHERE sn = '5']], tostring(q))

		q = TSQ.q():where_tag_isnot('sn', 5)
		assert.are.equal([[SELECT * FROM * WHERE sn != '5']], tostring(q))

		q = TSQ.q():where_tag_matches('sn', "[1234567890]+")
		assert.are.equal([[SELECT * FROM * WHERE sn =~ /[1234567890]+/]], tostring(q))

		q = TSQ.q():where_tag_not_matching('sn', "[1234567890]+")
		assert.are.equal([[SELECT * FROM * WHERE sn !~ /[1234567890]+/]], tostring(q))

		q = TSQ.q():where_tag_is('sn',5):OR_tag_is('sn', 7)
		assert.are.equal([[SELECT * FROM * WHERE ( sn = '5' OR sn = '7' )]], tostring(q))

		q = TSQ.q():where_tag_matches('sn', "."):AND_tag_is('sn', 12)
		assert.are.equal([[SELECT * FROM * WHERE sn =~ /./ AND sn = '12']], tostring(q))

		q = TSQ.q():where_tag_is('sn', 'a string')
		assert.are.equal([[SELECT * FROM * WHERE sn = 'a string']], tostring(q))

		q = TSQ.q():where_tag_is('sn', "a 'string")
		assert.are.equal([[SELECT * FROM * WHERE sn = 'a \'string']], tostring(q))

	end)
	it("checks escaping on tags", function()
		local q = TSQ.q():where_tag_is('sfeq235gse5y', "a 'string")
		assert.are.equal([[SELECT * FROM * WHERE sfeq235gse5y = 'a \'string']], tostring(q))

		q = TSQ.q():where_tag_is("bob's", "a 'string")
		assert.are.equal([[SELECT * FROM * WHERE "bob's" = 'a \'string']], tostring(q))

		q = TSQ.q():where_tag_is("€", "a 'string")
		assert.are.equal([[SELECT * FROM * WHERE "€" = 'a \'string']], tostring(q))

	end)

	it("checks field where clauses", function()
		local q
		q = TSQ.q():where_field_is('bob', 12)
		assert.are.equal([[SELECT * FROM * WHERE bob = 12]], tostring(q))

		q = TSQ.q():where_field_isnot('bob', 12)
		assert.are.equal([[SELECT * FROM * WHERE bob != 12]], tostring(q))

		q = TSQ.q():where_field_greater('bob', 12)
		assert.are.equal([[SELECT * FROM * WHERE bob > 12]], tostring(q))

		q = TSQ.q():where_field_less('bob', 12)
		assert.are.equal([[SELECT * FROM * WHERE bob < 12]], tostring(q))

		q = TSQ.q():where_field_is('bob', 'twelve')
		assert.are.equal([[SELECT * FROM * WHERE bob = 'twelve']], tostring(q))

		q = TSQ.q():where_field_isnot('bob', 'twelve')
		assert.are.equal([[SELECT * FROM * WHERE bob != 'twelve']], tostring(q))

		q = TSQ.q():where_field_isnot('bob', "twe'l've")
		assert.are.equal([[SELECT * FROM * WHERE bob != 'twe\'l\'ve']], tostring(q))

		-- should greater and less be tested for strings?

		q = TSQ.q():where_field_is('bob', true)
		assert.are.equal([[SELECT * FROM * WHERE bob = TRUE]], tostring(q))

		q = TSQ.q():where_field_isnot('bob', true)
		assert.are.equal([[SELECT * FROM * WHERE bob != TRUE]], tostring(q))

		q = TSQ.q():where_field_is('bob', false)
		assert.are.equal([[SELECT * FROM * WHERE bob = FALSE]], tostring(q))

		q = TSQ.q():where_field_isnot('bob', false)
		assert.are.equal([[SELECT * FROM * WHERE bob != FALSE]], tostring(q))

		--

		q = TSQ.q():where_field_is('d', 9):AND_field_isnot('h', 12)
		assert.are.equal([[SELECT * FROM * WHERE d = 9 AND h != 12]], tostring(q))

		q = TSQ.q():where_field_is('d', 9):OR_field_isnot('h', 12)
		assert.are.equal([[SELECT * FROM * WHERE ( d = 9 OR h != 12 )]], tostring(q))

	end)

	it("checks time where clauses.", function()
		local q
		q = TSQ.q():where_time_after('2d')
		assert.are.equal([[SELECT * FROM * WHERE time > 2d]], tostring(q))

		q = TSQ.q():where_time_before('2d')
		assert.are.equal([[SELECT * FROM * WHERE time < 2d]], tostring(q))

		q = TSQ.q():where_time_ago('2d')
		assert.are.equal([[SELECT * FROM * WHERE time > now() - 2d]], tostring(q))

		q = TSQ.q():where_time_since('2d')
		assert.are.equal([[SELECT * FROM * WHERE time < now() + 2d]], tostring(q))

	end)

	it("checks orderby clauses", function()
		local q
		q = TSQ.q():orderby('time')
		assert.are.equal([[SELECT * FROM * ORDER BY "time"]], tostring(q))

		q = TSQ.q():orderby('time', false)
		assert.are.equal([[SELECT * FROM * ORDER BY "time" DESC]], tostring(q))

		q = TSQ.q():orderby('time', 'desc')
		assert.are.equal([[SELECT * FROM * ORDER BY "time" DESC]], tostring(q))

		q = TSQ.q():orderby('time', true)
		assert.are.equal([[SELECT * FROM * ORDER BY "time" ASC]], tostring(q))

		q = TSQ.q():orderby('time', 'ASC')
		assert.are.equal([[SELECT * FROM * ORDER BY "time" ASC]], tostring(q))

	end)

	--------------------------------------------------------------
	it("parts can be added across multiple lines.", function()
		local q = TSQ.q()
		q:from("bob")
		q:fields("a")
		local s = tostring(q)
		assert.are.equal([[SELECT "a" FROM "bob"]], s)
	end)

	it("a complex one for a real query", function()
		local s
		s = TSQ.q():fields('MEAN(temp)'):from('wintd'):where_tag_is('sn',3):OR_tag_is('sn',5):AND_time_ago('1h'):groupby('sn'):groupbytime('15m'):fill('prev'):limit(1)
		local r = [[SELECT MEAN("temp") FROM "wintd" WHERE ( sn = '3' OR sn = '5' ) AND time > now() - 1h GROUP BY "sn",time( 15m ) fill(previous) LIMIT 1]]
		assert.are.equal(r, tostring(s))
	end)
	it("a complex one for a real query 2", function()
		local q
		q = TSQ.q():fields('MEAN(temp)'):from('wintd')
		q:where_tag_is('sn', 3):OR_tag_is('sn', 5)
		q:AND_time_ago('1h')
		q:groupby('sn'):groupbytime('15m'):fill('prev'):limit(1)
		local r = [[SELECT MEAN("temp") FROM "wintd" WHERE ( sn = '3' OR sn = '5' ) AND time > now() - 1h GROUP BY "sn",time( 15m ) fill(previous) LIMIT 1]]
		assert.are.equal(r, tostring(q))
	end)

	it("another real query to test.", function()
		local sn = 5
		local window = 30
		local r =  "SELECT * FROM \"wintd\" WHERE sn = '" ..sn.."' AND time > now() - " .. window .. "m LIMIT 10000"
		local qq = TSQ.q()
		qq:from('wintd')
		qq:where_tag_is('sn', sn)
		qq:AND_time_ago(window .. "m")
		qq:limit(10000)

		assert.are.equal(r, tostring(qq))

	end)
end)

-- vim: set ai sw=4 ts=4 :
