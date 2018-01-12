.PHONY: lint
lint:
	luacheck *.lua --no-redefined --no-unused-args

.PHONY: test
test:
	./test.lua
