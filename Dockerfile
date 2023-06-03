FROM golang:1.16-alpine as builder

WORKDIR /rosedb

# If you encounter some issues when pulling modules, \
# you can try to use GOPROXY, especially in China.
# ENV GOPROXY=https://goproxy.cn

COPY . .
RUN go build -o rosedb-server ./cmd/


FROM alpine:latest

WORKDIR /rosedb
COPY --from=builder /rosedb/rosedb-server /bin/rosedb-server

EXPOSE 5200:5200

ENTRYPOINT ["/bin/rosedb-server", "-host", "0.0.0.0"]

# Usage:

# build rosedb-server image
# docker build -t rosedb-server .

# print help
# docker run --rm --name rosedb-server -p 5200:5200 -it rosedb-server -h

# start rosedb-server container
# docker run --rm --name rosedb-server -p 5200:5200 -d rosedb-server 