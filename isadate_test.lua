

describe("Test the is_a_date function.", function()
	require "tsq"

	it("validates dates without times", function()
		assert.is_true( TSQ.is_a_date("1234-10-12"))
		assert.is_false(TSQ.is_a_date("1234-13-04"))
		assert.is_false(TSQ.is_a_date("1234-9-4"))
		assert.is_true(TSQ.is_a_date("1234-09-04"))
	end)

	it("validates dates with times", function()
		assert.is_true( TSQ.is_a_date("1234-10-12 12:33:24"))
		assert.is_true(TSQ.is_a_date("1234-09-04 23:33:24"))
		assert.is_true( TSQ.is_a_date("1234-10-12T12:33:24Z"))
		assert.is_true(TSQ.is_a_date("1234-09-04T23:33:24Z"))
		assert.is_false(TSQ.is_a_date("1234-09-04T23:33:24"))
	end)

	it("validates dates with times and nanoseconds", function()
		assert.is_true( TSQ.is_a_date("1234-10-12 12:33:24.2134"))
		assert.is_true(TSQ.is_a_date("1234-09-04 23:33:24.2134"))
		assert.is_true( TSQ.is_a_date("1234-10-12T12:33:24.2134Z"))
		assert.is_true(TSQ.is_a_date("1234-09-04T23:33:24.2134Z"))
		assert.is_false(TSQ.is_a_date("1234-09-04T23:33:24.2134"))
	end)

end)
-- vim: set ai sw=4 ts=4 :
