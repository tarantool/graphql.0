default:
	false

.PHONY: lint
lint:
	luacheck graphql/*.lua test/*/*.lua --no-redefined --no-unused-args

.PHONY: test
test: lint
	virtualenv -p python2.7 ./.env-2.7
	source ./.env-2.7/bin/activate && \
		pip install -r ./test-run/requirements.txt && \
		pip install tarantool && \
		cd test && ./test-run.py

.PHONY: clean
clean:
	rm -rf test/var
