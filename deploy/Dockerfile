# Builds the Moss service binaries (gateway + signaling relay) as static,
# CGO-free executables in a minimal distroless image. Used by the Fly.io configs
# in this directory, but works with any container host.
FROM golang:1.25 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/moss-gateway ./cmd/moss-gateway \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/moss-signal ./cmd/moss-signal

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/moss-gateway /out/moss-signal /usr/local/bin/
# Default command; Fly process groups (see fly.*.toml) override this.
ENTRYPOINT ["/usr/local/bin/moss-gateway"]
CMD ["-mesh", "moss", "-http", "0.0.0.0:8787"]
