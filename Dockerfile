# syntax=docker/dockerfile:1
FROM golang:1.26-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/spruce ./cmd/spruce && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/spruce-integration ./cmd/spruce-integration && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/spruce-bench ./cmd/spruce-bench

FROM scratch AS tools
COPY --from=build /out/spruce-integration /spruce-integration
COPY --from=build /out/spruce-bench /spruce-bench
USER 65532:65532

FROM scratch AS broker
COPY --from=build /out/spruce /spruce
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/spruce"]
