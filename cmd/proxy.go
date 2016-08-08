package main

import (
	"flag"
	"github.com/qgweb/redis-proxy"
	"github.com/ngaut/log"
	"net"
	_"net/http/pprof"
	"net/http"
)

var (
	confserver = flag.String("cserver", "http://127.0.0.1:13457", "配置服务器地址")
	phost = flag.String("host", "127.0.0.1", "监听地址")
	pport = flag.String("port", "13345", "监听端口")
	rconn  *rproxy.RedisConn
)

func init() {
	flag.Parse()
	if h, _ := rproxy.GetConfServer(*confserver); len(h) == 0 {
		log.Fatal("配置服务器连接失败,或没有可用redis连接")
	}
	h, _ := rproxy.GetConfServer(*confserver)
	rc, err := rproxy.NewRedisConn(h)
	if err != nil {
		log.Fatal(err)
	}
	rconn = rc
}

func handle(conn net.Conn) {
	hh, _ := rproxy.GetConfServer(*confserver)
	rc, err := rproxy.NewRedisConn(hh)
	if err != nil {
		log.Fatal(err)
	}
	h := rproxy.NewRConnHandle(conn, rc, 1000)
	h.Reader()
}

func main() {
	go func() {
	http.ListenAndServe("localhost:6060", nil)
	}()
	l, err := net.Listen("tcp", *phost + ":" + *pport)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Error(err)
			continue
		}

		go handle(conn)
	}
}