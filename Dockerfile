# syntax=docker/dockerfile:1
FROM golang:1.27.2-alpine@sha256:f92b6ef800e499660581efdabdf25d9d817a9d124eaf900924f0504e7e27e12d AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/spruce ./cmd/spruce && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/spruce-integration ./cmd/spruce-integration && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/spruce-lifecycle ./cmd/spruce-lifecycle && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/spruce-bench ./cmd/spruce-bench && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/spruce-binary-soak ./cmd/spruce-binary-soak

FROM scratch AS tools
COPY --from=build /out/spruce-integration /spruce-integration
COPY --from=build /out/spruce-lifecycle /spruce-lifecycle
COPY --from=build /out/spruce-bench /spruce-bench
COPY --from=build /out/spruce-binary-soak /spruce-binary-soak
USER 65532:65532

FROM python:3.14-alpine@sha256:c6ead215bfd31f1e433d968853b7a769989117115b728874824e6c0a27cb96fc AS python-probe
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
