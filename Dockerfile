# Stage 1: build both binaries
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/namenode ./cmd/namenode && \
    CGO_ENABLED=0 go build -o /out/datanode ./cmd/datanode

# Stage 2: small runtime image
FROM alpine:3.20
COPY --from=build /out/ /usr/local/bin/