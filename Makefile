default:
	false

.PHONY: lint
lint:
	luacheck *.lua --no-redefined --no-unused-args

.PHONY: test
test: lint
	./test.lua
	./test_space.lua

.PHONY: clean
clean:
	rm -f *.xlog *.snap
