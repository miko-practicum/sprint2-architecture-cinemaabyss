FROM golang:1.22-alpine AS build

WORKDIR /app

COPY go.mod ./
COPY main.go ./

RUN go build -o proxy-service main.go

FROM alpine:3.20

WORKDIR /app

COPY --from=build /app/proxy-service /app/proxy-service

EXPOSE 8000

CMD ["/app/proxy-service"]
