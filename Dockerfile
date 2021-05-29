FROM golang:alpine

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOPROXY=https://goproxy.cn,direct \
    PATH="/dist/bin:${PATH}"

WORKDIR /build

COPY . .

RUN go mod tidy && go build -o bin/rosedb-server cmd/server/main.go && go build -o bin/rosedb-cli cmd/cli/main.go

WORKDIR /dist

RUN cp -r /build/bin .

EXPOSE 5200

CMD ["/dist/bin/rosedb-server"]
