package main

import (
	"net"
	"github.com/ngaut/log"
	"github.com/qgweb/redis-proxy"
	"stathat.com/c/consistent"
	"bufio"
	"io"
)

var (
	pool *rproxy.Pool
	phash *consistent.Consistent
	pconnhash map[string]*rproxy.Pool
)

func init() {
	phash = consistent.New()
	pconnhash = make(map[string]*rproxy.Pool)
	phash.NumberOfReplicas = 20
	phash.Add("127.0.0.1:6379")
	phash.Add("127.0.0.1:6380")

	pconnhash["127.0.0.1:6379"] = rproxy.NewPool("127.0.0.1:6379", 10, func(addr string) (*rproxy.Conn, error) {
		return rproxy.NewConnection(addr, 30)
	})
	pconnhash["127.0.0.1:6380"] = rproxy.NewPool("127.0.0.1:6380", 10, func(addr string) (*rproxy.Conn, error) {
		return rproxy.NewConnection(addr, 30)
	})
}

func handle(conn net.Conn) {
	bi := bufio.NewReader(conn)
	for {
		resp, err := rproxy.Parse(bi)
		if err != nil && err.Error() == io.EOF.Error() {
			conn.Close()
			break
		}
		if err != nil {
			log.Error(err)
			conn.Close()
			break
		}

		op, keys, err := resp.GetOpKeys()
		if err != nil {
			log.Error(err)
			conn.Close()
			break
		}

		if string(op) == "PING" {
			conn.Write([]byte("+PONG\r\n"))
			continue
		}


		kk := keys[0]
		host, err := phash.Get(string(kk))
		if err != nil {
			log.Error(err)
			conn.Close()
			break
		}

		pconn, err := pconnhash[host].GetConn()
		if err != nil {
			log.Error(err)
			conn.Close()
			return
		}

		resp.WriteTo(pconn)
		pconn.Flush()

		resp, err = rproxy.Parse(pconn.BufioReader())
		if err != nil && err.Error() == io.EOF.Error() {
			log.Error(err)
			conn.Close()
			break
		}
		if err != nil {
			log.Error(err)
			conn.Close()
			break
		}
		resp.WriteTo(conn)
		pconnhash[host].PutConn(pconn)
	}
	//log.Error("closed")
}

func main() {
	l, err := net.Listen("tcp", "127.0.0.1:3344")
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
