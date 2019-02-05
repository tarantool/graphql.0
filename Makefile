WWW_BROWSER=$(shell \
	{ type xdg-open >/dev/null 2>&1 && echo "xdg-open"; } || \
	{ type open >/dev/null 2>&1 && echo "open"; } \
)

default:
	false

.PHONY: lint
lint:
	luacheck graphql/*.lua \
		graphql/core/execute.lua \
		graphql/core/rules.lua \
		graphql/core/validate_variables.lua \
		graphql/convert_schema/*.lua \
		graphql/expressions/*.lua \
		graphql/server/*.lua \
		test/bench/*.lua \
		test/space/*.lua \
		test/testdata/*.lua \
		test/common/*.lua \
		test/extra/*.lua \
		test/*.lua \
		--no-redefined --no-unused-args

.PHONY: apidoc-lint
apidoc-lint:
	! ldoc -d doc/apidoc-lint-tmp graphql --all -f markdown 2>&1 >/dev/null | \
		grep -v -e 'graphql/core.*: no module() call found; no initial doc comment$$' \
		-e ': contains no items$$'
	rm -rf doc/apidoc-lint-tmp

.PHONY: test
test: lint apidoc-lint
	virtualenv -p python2.7 ./.env-2.7
	. ./.env-2.7/bin/activate && \
		pip install -r ./test-run/requirements.txt && \
		pip install tarantool && \
		cd test && ./test-run.py

.PHONY: bench
bench: lint apidoc-lint
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
	rm -rf test/var luacov.stats.out luacov.report.out

.PHONY: apidoc
apidoc: apidoc-lint
	ldoc -d doc/apidoc graphql --all -f markdown
	# fix navigation panel width
	sed -i -e 's/: 14em;/: 24em;/' doc/apidoc/ldoc.css

.PHONY: serve-apidoc
serve-apidoc: apidoc
	$(WWW_BROWSER) ./doc/apidoc/index.html

.PHONY: demo
demo:
	./demo/demo.lua $(TESTDATA)

.PHONY: rpm
rpm:
	OS=el DIST=7 PACKAGECLOUD_USER=tarantool PACKAGECLOUD_REPO=1_9 \
	   ./3rd_party/packpack/packpack

.PHONY: coverage
coverage: lint apidoc-lint
	virtualenv -p python2.7 ./.env-2.7
	. ./.env-2.7/bin/activate && \
		pip install -r ./test-run/requirements.txt && \
		pip install tarantool && \
		cd test && ./test-run.py --luacov --no-output-timeout 600
	find test/var -name luacov.stats.out | xargs -I {} sed \
		-e 's@/test/[^ /]\+/\.\./\.\./graphql@/graphql@' \
		-e 's@/test/../graphql@/graphql@' \
		-e 's@'"$$(realpath .)"'/@@' \
		-i {}
	find test/var -name luacov.stats.out | xargs \
		tools/luacov_merge.lua # create luacov.stats.out
	luacov ^graphql  # generate luacov.report.out
	awk '/^Summary$$/{i=1;}{if(i){print;}}' luacov.report.out
