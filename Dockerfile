FROM migrate/migrate:v4.18.3 AS migrate
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /crawler ./cmd/crawler
RUN CGO_ENABLED=0 go build -o /migration ./cmd/migration
FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /crawler /crawler
COPY --from=build /migration /migration
COPY --from=migrate /usr/local/bin/migrate /usr/local/bin/migrate
COPY migrations/ /migrations/
EXPOSE 8081
ENTRYPOINT ["/crawler"]
