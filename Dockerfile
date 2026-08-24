# syntax=docker/dockerfile:1
FROM golang:1.26.6-alpine@sha256:3889b425f035be855a72fb4755265311293b6d414521f0a519d819df32222d83 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/spruce ./cmd/spruce && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/spruce-integration ./cmd/spruce-integration && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/spruce-bench ./cmd/spruce-bench && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/spruce-binary-soak ./cmd/spruce-binary-soak

FROM scratch AS tools
COPY --from=build /out/spruce-integration /spruce-integration
COPY --from=build /out/spruce-bench /spruce-bench
COPY --from=build /out/spruce-binary-soak /spruce-binary-soak
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
