# Build Stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Install certificates and build dependencies
RUN apk add --no-cache ca-certificates git

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application with security flags
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o main cmd/main.go

# Final Stage - Distroless with nonroot
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/main .

# Copy CA certificates for HTTPS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Run as nonroot user (UID 65532)
USER nonroot:nonroot

# Expose port (if applicable)
# EXPOSE 8080

ENTRYPOINT ["./main"]
