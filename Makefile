WWW_BROWSER=$(shell \
	{ type xdg-open >/dev/null 2>&1 && echo "xdg-open"; } || \
	{ type open >/dev/null 2>&1 && echo "open"; } \
)

default:
	false

.PHONY: lint
lint:
	luacheck graphql/*.lua \
		test/bench/*.lua \
		test/local/*.lua \
		test/testdata/*.lua \
		test/common/*.test.lua test/common/lua/*.lua \
		test/extra/*.test.lua \
		test/*.lua \
		--no-redefined --no-unused-args

.PHONY: test
test: lint
	virtualenv -p python2.7 ./.env-2.7
	. ./.env-2.7/bin/activate && \
		pip install -r ./test-run/requirements.txt && \
		pip install tarantool && \
		cd test && ./test-run.py

.PHONY: bench
bench: lint
	virtualenv -p python2.7 ./.env-2.7
	. ./.env-2.7/bin/activate && \
		pip install -r ./test-run/requirements.txt && \
		pip install tarantool && \
		cd test && ./test-run.py --long bench/

.PHONY: pure-test
pure-test:
	cd test && ./test-run.py

.PHONY: pure-bench
pure-bench:
	cd test && ./test-run.py --long bench/

.PHONY: clean
clean:
	rm -rf test/var

.PHONY: apidoc
apidoc:
	ldoc -d doc/apidoc graphql --all -f markdown
	# fix navigation panel width
	sed -i -e 's/: 14em;/: 24em;/' doc/apidoc/ldoc.css

.PHONY: serve-apidoc
serve-apidoc: apidoc
	$(WWW_BROWSER) ./doc/apidoc/index.html
