describe("Write string generation", function()
	require 'tsw'
	it("make a write string", function()
		local s
		s = TSW.write('bob', {sn=9}, {a=54})
		r = [[bob,sn=9 a=54]]
		assert.are.equal(r, s)
	end)
end)
-- vim: set ai sw=4 ts=4 :
