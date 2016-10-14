-- luacheck: globals describe it (busted globals)
describe("Write string generation", function()
	require 'tsw'
	it("make a write string", function()
		local s
		s = TSW.write('bob', {sn=9}, {a=54})
		local r = [[bob,sn=9 a=54]]
		assert.are.equal(r, s)
	end)

	it("optionally has a timestamp", function()
		local s
		s = TSW.write('bob', {sn=9}, {a=54}, 1234567890)
		local r = [[bob,sn=9 a=54 1234567890]]
		assert.are.equal(r, s)
	end)

end)
-- vim: set ai sw=4 ts=4 :
