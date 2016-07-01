
describe("Test the series results iterator", function()
	require "tsq"

	it("can get the next value in a single series", function()
		local sr = {
			series={
				{
					columns={"time","mean"},
					name = "wintd",
					values={{14673141, 77}, {14673142, 90} }
				}
			}
		}
		local i, t
		i, t = TSQ.next_in_series(sr.series)
		assert.are.equal(1, i)
		assert.are.equal(t.time, 14673141)
		assert.are.equal(t.mean, 77)

		i, t = TSQ.next_in_series(sr.series, 1)
		assert.are.equal(2, i)
		assert.are.equal(t.time, 14673142)
		assert.are.equal(t.mean, 90)

		i, t = TSQ.next_in_series(sr.series, 2)
		assert.is_nil(i)
		assert.is_nil(t)
	end)

	it("over a single series", function()
		local sr = {
			series={
				{
					columns={"time","mean"},
					name = "wintd",
					values={{14673141, 77.76}, {14673142, 90} }
				}
			}
		}
		local limit = 3
		for t in tsq_result_i(sr) do

			-- make sure the iterator stops.
			limit = limit - 1
			assert.is_true( limit > 0 )
		end
	end)
end)

-- vim: set ai sw=4 ts=4 :
