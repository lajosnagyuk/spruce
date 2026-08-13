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

FROM python:3.14-alpine@sha256:05b2b8b732ecd268fee8727a369f936f022d1321b59befd13c30ede22769dcdc AS python-probe
WORKDIR /app
COPY clients/python /src
RUN pip install --no-cache-dir /src && \
    pip uninstall -y msgpack setuptools && \
    python -m pip uninstall -y pip && \
    find /src -type d -name '*.egg-info' -exec rm -rf {} +
COPY clients/python/tests/cluster_probe.py /app/cluster_probe.py
USER 65532:65532
ENTRYPOINT ["python", "/app/cluster_probe.py"]

FROM scratch AS broker
COPY --from=build /out/spruce /spruce
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/spruce"]
