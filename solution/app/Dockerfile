FROM golang:1.23-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /api ./cmd/api && CGO_ENABLED=0 go build -o /worker ./cmd/worker

FROM alpine:3.20
COPY --from=build /api /usr/local/bin/api
COPY --from=build /worker /usr/local/bin/worker
CMD ["/usr/local/bin/api"]
