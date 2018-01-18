.PHONY: lint
lint:
	luacheck *.lua --no-redefined --no-unused-args

.PHONY: test
test: lint
	./test.lua