
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

	it("can get the next value in multiple series", function()
		local sr = {
			series={
				{
					columns={"time","mean"},
					name = "wintd",
					values={{14673141, 77}, {14673142, 90} }
				},
				{
					columns={"time","mean"},
					name = "wintd",
					values={{14673166, 66}, {14673167, 45} }
				},
			}
		}
		local i, t, u
		i, t, u = TSQ.next_in_series(sr.series)
		assert.are.equal(1, i)
		assert.are.equal(t.time, 14673141)
		assert.are.equal(t.mean, 77)
		assert.are.equal(u.time, 14673166)
		assert.are.equal(u.mean, 66)

		i, t, u = TSQ.next_in_series(sr.series, 1)
		assert.are.equal(2, i)
		assert.are.equal(t.time, 14673142)
		assert.are.equal(t.mean, 90)
		assert.are.equal(u.time, 14673167)
		assert.are.equal(u.mean, 45)

		i, t, u = TSQ.next_in_series(sr.series, 2)
		assert.is_nil(i)
		assert.is_nil(t)
		assert.is_nil(u)
	end)

	it("can get the next value in multiple uneven series", function()
		local sr = {
			series={
				{
					columns={"time","mean"},
					name = "wintd",
					values={{14673141, 77}, {14673142, 90}, {14673143, 55} }
				},
				{
					columns={"time","mean"},
					name = "wintd",
					values={{14673166, 66}, {14673167, 45} }
				},
			}
		}
		local i, t, u
		i, t, u = TSQ.next_in_series(sr.series)
		assert.are.equal(1, i)
		assert.are.equal(t.time, 14673141)
		assert.are.equal(t.mean, 77)
		assert.are.equal(u.time, 14673166)
		assert.are.equal(u.mean, 66)

		i, t, u = TSQ.next_in_series(sr.series, 1)
		assert.are.equal(2, i)
		assert.are.equal(t.time, 14673142)
		assert.are.equal(t.mean, 90)
		assert.are.equal(u.time, 14673167)
		assert.are.equal(u.mean, 45)

		i, t, u = TSQ.next_in_series(sr.series, 2)
		assert.are.equal(3, i)
		assert.are.equal(t.time, 14673143)
		assert.are.equal(t.mean, 55)
		assert.are.same({}, u)

		i, t, u = TSQ.next_in_series(sr.series, 3)
		assert.is_nil(i)
		assert.is_nil(t)
		assert.is_nil(u)
	end)

	it("can get the next value in multiple uneven series again", function()
		local sr = {
			series={
				{
					columns={"time","mean"},
					name = "wintd",
					values={{14673141, 77}, {14673142, 90} }
				},
				{
					columns={"time","mean"},
					name = "wintd",
					values={{14673166, 66}, {14673167, 45}, {14673143, 55} }
				},
			}
		}
		local i, t, u
		i, t, u = TSQ.next_in_series(sr.series)
		assert.are.equal(1, i)
		assert.are.equal(t.time, 14673141)
		assert.are.equal(t.mean, 77)
		assert.are.equal(u.time, 14673166)
		assert.are.equal(u.mean, 66)

		i, t, u = TSQ.next_in_series(sr.series, 1)
		assert.are.equal(2, i)
		assert.are.equal(t.time, 14673142)
		assert.are.equal(t.mean, 90)
		assert.are.equal(u.time, 14673167)
		assert.are.equal(u.mean, 45)

		i, t, u = TSQ.next_in_series(sr.series, 2)
		assert.are.equal(3, i)
		assert.are.equal(u.time, 14673143)
		assert.are.equal(u.mean, 55)
		assert.are.same({}, t)

		i, t, u = TSQ.next_in_series(sr.series, 3)
		assert.is_nil(i)
		assert.is_nil(t)
		assert.is_nil(u)
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
