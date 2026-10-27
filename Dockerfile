FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /crawler ./cmd/crawler
FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /crawler /crawler
EXPOSE 8081
ENTRYPOINT ["/crawler"]
