FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/translation-api .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/translation-api /translation-api
EXPOSE 8000
USER nonroot:nonroot
ENTRYPOINT ["/translation-api"]
