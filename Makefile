.PHONY: proto build test up down logs kill1 revive1

# Generates Go code for every .proto file under proto/. Milestone 3 adds
# proto/kv/kv.proto; proto/raft/raft.proto doesn't exist until Milestone 5,
# so we glob for whatever .proto files actually exist instead of naming them
# explicitly here.
#
# protoc-gen-go / protoc-gen-go-grpc are Go binaries installed to
# $(go env GOPATH)/bin via `go install`, which protoc shells out to as
# plugins. That directory isn't necessarily on PATH, so we prepend it just
# for this recipe rather than requiring every contributor to edit their
# shell profile.
proto:
	@PROTO_FILES="$$(find proto -name '*.proto' 2>/dev/null)"; \
	if [ -z "$$PROTO_FILES" ]; then \
		echo "proto: no .proto files yet — nothing to generate"; \
	else \
		PATH="$$(go env GOPATH)/bin:$$PATH" protoc \
			--go_out=. --go_opt=paths=source_relative \
			--go-grpc_out=. --go-grpc_opt=paths=source_relative \
			$$PROTO_FILES; \
	fi

build:
	go build -o bin/node ./cmd/node

test:
	go test -race ./...

up:
	docker compose up --build

down:
	docker compose down -v

logs:
	docker compose logs -f

kill1:
	docker compose stop node1

revive1:
	docker compose start node1
