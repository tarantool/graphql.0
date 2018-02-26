default:
	false

.PHONY: lint
lint:
	luacheck graphql/*.lua test/local/*.lua test/testdata/*.lua \
		test/common/*.test.lua test/common/lua/*.lua \
		--no-redefined --no-unused-args

.PHONY: test
test: lint
	virtualenv -p python2.7 ./.env-2.7
	. ./.env-2.7/bin/activate && \
		pip install -r ./test-run/requirements.txt && \
		pip install tarantool && \
		cd test && ./test-run.py

.PHONY: pure-test
pure-test:
	cd test && ./test-run.py

.PHONY: clean
clean:
	rm -rf test/var

.PHONY: apidoc
apidoc:
	ldoc -d doc/apidoc graphql --all -f markdown
	# fix navigation panel width
	sed -i -e 's/: 14em;/: 24em;/' doc/apidoc/ldoc.css
