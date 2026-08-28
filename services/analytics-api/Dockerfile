FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.23-alpine AS build
WORKDIR /src
ARG TARGETOS
ARG TARGETARCH
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -ldflags="-s -w" -o /out/analytics-api .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/analytics-api /analytics-api
EXPOSE 8001
USER nonroot:nonroot
ENTRYPOINT ["/analytics-api"]
