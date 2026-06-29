FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o sentinel-v3 .

FROM gcr.io/distroless/static-debian12

COPY --from=builder /app/sentinel-v3 /sentinel-v3
COPY --from=builder /app/catalog/catalog.json /app/catalog/catalog.json

EXPOSE 8080
ENTRYPOINT ["/sentinel-v3"]
