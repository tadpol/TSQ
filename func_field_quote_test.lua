
describe("Query generation", function()
	require "tsq"

	it("handles no function.", function()
		local q
		q = TSQ.func_field_quoter("water")
		assert.are.equal([["water"]], q)

		q = TSQ.func_field_quoter('"water"')
		assert.are.equal([["water"]], q)
	end)

	it("handles one function.", function()
		local q
		q = TSQ.func_field_quoter("MEAN(water)")
		assert.are.equal([[MEAN("water")]], q)

		q = TSQ.func_field_quoter('MEAN("water")')
		assert.are.equal([[MEAN("water")]], q)
	end)

	it("handles one function with multiple params", function()
		local q
		q = TSQ.func_field_quoter("TOP(water, 4)")
		assert.are.equal([[TOP("water", 4)]], q)

		q = TSQ.func_field_quoter("TOP(water,4)")
		assert.are.equal([[TOP("water",4)]], q)

		q = TSQ.func_field_quoter('TOP("water", 4)')
		assert.are.equal([[TOP("water", 4)]], q)
	end)

	it("handles nested functions.", function()
		local q
		q = TSQ.func_field_quoter("DERIVATIVE(MEAN(water))")
		assert.are.equal([[DERIVATIVE(MEAN("water"))]], q)

		q = TSQ.func_field_quoter('DERIVATIVE(MEAN("water"))')
		assert.are.equal([[DERIVATIVE(MEAN("water"))]], q)
	end)

	it("handles nested functions with multiple params", function()
		local q
		q = TSQ.func_field_quoter("DERIVATIVE(MEAN(water), 4m)")
		assert.are.equal([[DERIVATIVE(MEAN("water"), 4m)]], q)

		q = TSQ.func_field_quoter('DERIVATIVE(MEAN("water"), 4m)')
		assert.are.equal([[DERIVATIVE(MEAN("water"), 4m)]], q)

		q = TSQ.func_field_quoter("DERIVATIVE(MEAN(water, 6), 4m)")
		assert.are.equal([[DERIVATIVE(MEAN("water", 6), 4m)]], q)

	end)

end)
-- vim: set ai sw=4 ts=4 :
