TOOLS := alert-proxy backport-verifier cluster-manifest-verifier commenter ci-scheduling-webhook determinize-peribolos gpu-scheduling-webhook helpdesk-faq pipeline-controller pr-reminder publicize retester

.PHONY: build-all test clean format gofmt lint validate-modules $(addprefix build-,$(TOOLS)) $(addprefix image-,$(TOOLS))

build-all: $(addprefix build-,$(TOOLS))

build-cluster-manifest-verifier:
	cd cmd/cluster-manifest-verifier && go build -o $(CURDIR)/_output/cluster-manifest-verifier .

build-%:
	go build -o _output/$* ./cmd/$*/

production-install:
	for tool in $(TOOLS); do \
		if [ "$$tool" = "cluster-manifest-verifier" ]; then \
			(cd cmd/cluster-manifest-verifier && go install .); \
		else \
			go install ./cmd/$$tool/; \
		fi; \
	done
.PHONY: production-install

test:
	LANG=C LC_ALL=C go test ./...
.PHONY: test

format: gofmt
.PHONY: format

gofmt:
	gofmt -s -w $(shell go list -f '{{ .Dir }}' ./... )
.PHONY: gofmt

lint:
	./hack/lint.sh
.PHONY: lint

validate-modules:
	go mod tidy
	@if ! git diff --exit-code go.mod go.sum; then \
		echo "modules are out of date, run 'go mod tidy'"; exit 1; \
	fi
.PHONY: validate-modules

clean:
	rm -rf _output

define image-rule
image-$(1): build-$(1)
	cp _output/$(1) images/$(1)/$(1)
	podman build -t $(1) images/$(1)/
	rm -f images/$(1)/$(1)
endef

$(foreach tool,$(TOOLS),$(eval $(call image-rule,$(tool))))
