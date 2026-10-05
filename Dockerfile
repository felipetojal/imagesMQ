# Um único Dockerfile para os 3 serviços: use --build-arg SERVICE=client|converter|storage
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY internal ./internal
COPY cmd ./cmd
RUN go mod tidy && go mod download
ARG SERVICE
RUN CGO_ENABLED=0 go build -o /out/app ./cmd/${SERVICE}

FROM alpine:3.20
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
