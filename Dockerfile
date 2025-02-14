# Use Go 1.22 as the build stage
FROM golang:1.22-alpine AS builder

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum to the working directory
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the executable
RUN go build -o sync cmd/sync/main.go

# Use a smaller base image to run the application
FROM alpine:latest

# Set timezone (optional)
RUN apk add --no-cache tzdata
ENV TZ=Asia/Tokyo

# Set the working directory
WORKDIR /app

# Copy the executable
COPY --from=builder /app/sync .

# Copy the configuration file
COPY --from=builder /app/sync.db ./sync.db

# Run the application
ENTRYPOINT ["./sync"]