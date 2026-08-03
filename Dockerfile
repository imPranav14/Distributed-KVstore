# Build stage: compile a static Go binary. Using a full Go image here (not
# the final runtime image) keeps the shipped container small and avoids
# bundling the Go toolchain into every node's image.
FROM golang:1.22 AS build
WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 produces a statically linked binary with no libc dependency,
# so it runs unmodified on the minimal base image below.
RUN CGO_ENABLED=0 go build -o /out/node ./cmd/node

# Runtime stage: just the binary, nothing else. Smaller image, smaller
# attack surface, faster `docker compose up`.
FROM alpine:3.19
COPY --from=build /out/node /usr/local/bin/node

ENTRYPOINT ["/usr/local/bin/node"]
