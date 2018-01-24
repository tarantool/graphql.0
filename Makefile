default:
	false

.PHONY: lint
lint:
	luacheck {graphql,test}/*.lua --no-redefined --no-unused-args

.PHONY: test
test: lint
	./test/simple.test.lua
	./test/space.test.lua

.PHONY: clean
clean:
	rm -f {,test/}*.{xlog,snap}
