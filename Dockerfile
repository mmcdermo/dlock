FROM golang

COPY . /go/src/dlock
RUN cd /go/src/dlock/ && go get -d -v \
    && cd /go/src/dlock/server/ && go get -d -v

RUN go build /go/src/dlock/server/dlock_server.go

ENTRYPOINT /go/src/dlock/server/dlock_server

EXPOSE 8422