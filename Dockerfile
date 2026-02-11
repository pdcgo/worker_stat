FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o stat ./cmd/stat

FROM alpine:latest
COPY --from=builder /app/stat /stat
CMD ["/stat", "batch"]