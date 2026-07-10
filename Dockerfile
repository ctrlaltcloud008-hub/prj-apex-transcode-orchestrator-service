FROM golang:1.26.3 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /transcode-orchestrator ./cmd

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /transcode-orchestrator /app/transcode-orchestrator
COPY --from=build /src/config.yaml /app/config.yaml

ENV PORT=8080
EXPOSE 8080

ENTRYPOINT ["/app/transcode-orchestrator"]
