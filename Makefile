#
# |_  _  _ _ _  _  _ _ _  _ _|_
# |_)(_)_\| | |(_|| | | |(_) |
#
# Bosmarmot Makefile
#
# Requires go version 1.8 or later.
#

SHELL := /bin/bash
REPO := $(shell pwd)
GO_FILES := $(shell go list -f "{{.Dir}}" ./...)
GOPACKAGES_NOVENDOR := $(shell go list ./...)
COMMIT := $(shell git rev-parse --short HEAD)
BURROW_PACKAGE := github.com/hyperledger/burrow
CI_IMAGE := quay.io/monax/bosmarmot:ci

### Integration test binaries
# We make the relevant targets for building/fetching these depend on the Makefile itself - if unnecessary rebuilds
# when changing the Makefile become a problem we can move these values into individual files elsewhere and make those
# files specific targets for their respective binaries

### Tests and checks
# Run goimports (also checks formatting) first display output first, then check for success
.PHONY: check
check:
	@go get golang.org/x/tools/cmd/goimports
	@goimports -l -d ${GO_FILES}
	@goimports -l ${GO_FILES} | read && echo && \
	echo "Your marmot has found a problem with the formatting style of the code."\
	 1>&2 && exit 1 || true

# Just fix it
.PHONY: fix
fix:
	@goimports -l -w ${GO_FILES}

.PHONY: test_js
test_js:
	@cd burrow.js && npm test

# Run tests
.PHONY:	test_bos
test_bos: check bin/solc
	@tests/scripts/bin_wrapper.sh go test ${GOPACKAGES_NOVENDOR}

.PHONY:	test
test: test_bos

# Run tests for development (noisy)
.PHONY:	test_dev
test_dev:
	@go test -v ${GOPACKAGES_NOVENDOR}

# Install dependency
.PHONY: npm_install
npm_install:
	@cd burrow.js && npm install

# Run tests including integration tests
.PHONY:	test_integration_vent
test_integration_vent:
	@GOCACHE=off go test -tags integration ./vent/...

.PHONY:	test_burrow_js
test_burrow_js: bin/solc bin/burrow
	@cd burrow.js && ../tests/scripts/bin_wrapper.sh npm test

.PHONY:	test_integration
test_integration: test_integration_vent test_burrow_js

# Use a provided/local Burrow
.PHONY:	test_burrow_js_no_burrow
test_burrow_js_no_burrow: bin/solc
	@cd burrow.js && ../tests/scripts/bin_wrapper.sh npm test

PHONY:	test_integration_no_burrow
test_integration_no_burrow: test_burrow_js_no_burrow

### Vendoring

# erase vendor wipes the full vendor directory
.PHONY: erase_vendor
erase_vendor:
	rm -rf ${REPO}/vendor/

# install vendor uses dep to install vendored dependencies
.PHONY: reinstall_vendor
reinstall_vendor: erase_vendor
	@dep ensure -v

# delete the vendor directy and pull back using dep lock and constraints file
# will exit with an error if the working directory is not clean (any missing files or new
# untracked ones)
.PHONY: ensure_vendor
ensure_vendor: reinstall_vendor
	@tests/scripts/is_checkout_dirty.sh

### Builds

.PHONY: protobuf
protobuf:
	@rm -rf ${REPO}/burrow.js/protobuf
	@cp -a ${REPO}/vendor/github.com/hyperledger/burrow/protobuf ${REPO}/burrow.js/protobuf

bin/solc: ./tests/scripts/deps/solc.sh
	@mkdir -p bin
	@tests/scripts/deps/solc.sh bin/solc
	@touch bin/solc

tests/scripts/deps/burrow.sh: Gopkg.lock
	@go get -u github.com/golang/dep/cmd/dep
	@tests/scripts/deps/burrow-gen.sh > tests/scripts/deps/burrow.sh
	@chmod +x tests/scripts/deps/burrow.sh

.PHONY: burrow_local
burrow_local:
	@rm -rf .gopath_burrow
	@mkdir -p .gopath_burrow/src/${BURROW_PACKAGE}
	@cp -r ${GOPATH}/src/${BURROW_PACKAGE}/. .gopath_burrow/src/${BURROW_PACKAGE}

bin/burrow: ./tests/scripts/deps/burrow.sh
	mkdir -p bin
	@GOPATH="${REPO}/.gopath_burrow" \
	tests/scripts/go_get_revision.sh \
	https://github.com/hyperledger/burrow.git \
	${BURROW_PACKAGE} \
	$(shell ./tests/scripts/deps/burrow.sh) \
	"make build" && \
	cp .gopath_burrow/src/${BURROW_PACKAGE}/bin/burrow ./bin/burrow


# Build all the things

# Build binaries for all architectures
.PHONY: build_dist
build_dist:
	@goreleaser --rm-dist --skip-publish --skip-validate

# Do all available tests and checks then build
.PHONY: build_ci
build_ci: check test build

# Tag the current HEAD commit with the current release defined in
# ./release/release.go
.PHONY: tag_release
tag_release: test check
	@tests/scripts/tag_release.sh

# If the checked out commit is tagged with a version then release to github
.PHONY: release
release: NOTES.md
	@tests/scripts/release.sh

.PHONY: build_ci_image
build_ci_image:
	docker build -t ${CI_IMAGE} -f ./.circleci/Dockerfile .

.PHONY: build_ci_image_no_cache
build_ci_image_no_cache:
	docker build --no-cache -t ${CI_IMAGE} -f ./.circleci/Dockerfile .

.PHONY: push_ci_image
push_ci_image: build_ci_image
	docker push ${CI_IMAGE}
