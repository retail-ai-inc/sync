FROM golang:1.23-alpine AS builder

# Install unzip tool for extracting the UI files
RUN apk add --no-cache unzip

# Set the working directory
WORKDIR /app

# Install gcc and other build dependencies
RUN apk add --no-cache gcc musl-dev

# Copy go.mod and go.sum to the working directory
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the executable
ENV CGO_ENABLED=1
RUN go build -o sync cmd/sync/main.go

# Extract the UI files during the build stage
RUN mkdir -p /app/ui && unzip -o /app/ui/dist.zip -d /app/ui/

# Use a smaller base image to run the application
FROM alpine:latest

# Install tzdata (optional)
RUN apk add --no-cache tzdata sqlite
ENV TZ=Asia/Tokyo

# Set the working directory
WORKDIR /app

# Copy the executable
COPY --from=builder /app/sync .

# Copy the configuration file
COPY --from=builder /app/sync.db ./sync.db

# Copy the extracted UI files
COPY --from=builder /app/ui /app/ui

# Run the application
ENTRYPOINT ["./sync"]
