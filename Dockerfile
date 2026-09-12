FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /anomaly .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /anomaly /anomaly
COPY config.yaml /etc/anomaly/config.yaml
EXPOSE 8080
ENTRYPOINT ["/anomaly"]
CMD ["-config", "/etc/anomaly/config.yaml", "-serve", ":8080"]
