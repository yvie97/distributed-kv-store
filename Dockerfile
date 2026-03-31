# DistKV Dockerfile - QUICK START VERSION
#
# This is a simple Dockerfile for quick local builds.
# For production-ready version with entrypoint scripts, see: deploy/docker/Dockerfile
#
# Multi-stage build for optimal image size

# Build stage
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code (includes pre-generated proto files)
COPY . .

# Build binaries
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o distkv-server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o distkv-client ./cmd/client

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1001 distkv && \
    adduser -D -u 1001 -G distkv distkv

# Set working directory
WORKDIR /app

# Copy binaries from builder
COPY --from=builder /app/distkv-server .
COPY --from=builder /app/distkv-client .

# Create data directory
RUN mkdir -p /data && chown -R distkv:distkv /data /app

# Switch to non-root user
USER distkv

# Expose default port
EXPOSE 8080

# Set default data directory
ENV DATA_DIR=/data

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ./distkv-client -server=localhost:8080 status || exit 1

# Default command
CMD ["./distkv-server", "-node-id=docker-node", "-address=0.0.0.0:8080", "-data-dir=/data"]

# Labels for metadata
LABEL maintainer="DistKV Team"
LABEL description="DistKV - Distributed Key-Value Store"
LABEL version="1.0"