
describe("Test the series results iterator", function()
	require "tsq"

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
