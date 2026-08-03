.PHONY: proto build test up down logs kill1 revive1

# Milestone 3 adds proto/kv/kv.proto and proto/raft/raft.proto. Until then
# there is nothing to generate, so this target just says so instead of
# failing with a confusing "no such file" error from protoc.
proto:
	@if [ -z "$$(find proto -name '*.proto' 2>/dev/null)" ]; then \
		echo "proto: no .proto files yet (added in Milestone 3) — nothing to generate"; \
	else \
		protoc --go_out=. --go_opt=paths=source_relative \
			--go-grpc_out=. --go-grpc_opt=paths=source_relative \
			proto/kv/kv.proto proto/raft/raft.proto; \
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
