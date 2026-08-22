FROM golang:1.23-alpine AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
COPY main.go ./

RUN go mod download
RUN go build -o events-service main.go

FROM alpine:3.20

WORKDIR /app

COPY --from=build /app/events-service /app/events-service

EXPOSE 8082

CMD ["/app/events-service"]
