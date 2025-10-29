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

# Install runtime dependencies including MongoDB tools, MySQL tools and Google Cloud SDK
RUN apk update && apk add --no-cache \
    tzdata \
    sqlite \
    mongodb-tools \
    mysql-client \
    python3 \
    py3-pip \
    curl \
    bash \
    zip

# Install Google Cloud SDK for gsutil command
RUN curl https://sdk.cloud.google.com | bash
ENV PATH $PATH:/root/google-cloud-sdk/bin

ENV TZ=Asia/Tokyo

# Set the working directory
WORKDIR /app

# Copy the executable
COPY --from=builder /app/sync .

# Copy the configuration file
COPY --from=builder /app/sync.db /mnt/state/sync.db

ENV SYNC_DB_PATH=/mnt/state/sync.db

# Copy the extracted UI files
COPY --from=builder /app/ui /app/ui

# Copy the cloudbuild.sh script for Slack notifications
COPY cloudbuild.sh /app/cloudbuild.sh
RUN chmod +x /app/cloudbuild.sh

# Copy and setup the startup script
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Run the startup script
ENTRYPOINT ["/app/start.sh"]
