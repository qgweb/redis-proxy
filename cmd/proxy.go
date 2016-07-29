package main

import (
	"net"
	"github.com/ngaut/log"
	"github.com/qgweb/redis-proxy"
	"stathat.com/c/consistent"
	"github.com/astaxie/beego/httplib"
	"bufio"
	"io"
	"flag"
	"sync"
	"strings"
	"time"
)

var (
	pool *rproxy.Pool
	phash *consistent.Consistent
	pconnhash map[string]*rproxy.Pool

	confserver = flag.String("cserver", "http://127.0.0.1:13457", "配置服务器地址")
	phost = flag.String("host", "127.0.0.1", "监听地址")
	pport = flag.String("port", "13345", "监听端口")
	serverhost []string
	mux sync.RWMutex
	pmux sync.RWMutex
)

func init() {
	flag.Parse()
	mux = sync.RWMutex{}
	pmux = sync.RWMutex{}
	serverhost = make([]string, 0)
	if len(getConfServer()) == 0 {
		log.Fatal("配置服务器请求失败！！")
	}
	initPoolHash()
}

func getConfServer() []string {
	mux.Lock()
	defer mux.Unlock()
	resp := httplib.Get(*confserver)
	s, err := resp.String()
	if err != nil {
		log.Error(err, "无法获取配置服务器内容")
		return nil
	}
	return strings.Split(s, ",")
}

func initPoolHash() {
	pmux.Lock()
	defer pmux.Unlock()
	h := consistent.New()
	pch := make(map[string]*rproxy.Pool)
	ss := serverhost
	sh := getConfServer()
	if strings.Join(ss, "") == strings.Join(sh, "") {
		return
	}
	serverhost = sh
	h.NumberOfReplicas = len(sh) * 10
	for _, v := range sh {
		h.Add(v)
		pch[v] = rproxy.NewPool(v, 10, func(addr string) (*rproxy.Conn, error) {
			return rproxy.NewConnection(addr, 30)
		})
	}

	phash = h
	pconnhash = pch
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
		pmux.RLock()

		host, err := phash.Get(string(kk))
		if err != nil {
			pmux.RUnlock()
			log.Error(err)
			conn.Close()
			break
		}

		pconn, err := pconnhash[host].GetConn()
		if err != nil {
			pmux.RUnlock()
			log.Error(err)
			conn.Close()
			return
		}

		pconn.SetWriteDeadline(time.Now().Add(time.Second * 30))
		resp.WriteTo(pconn)
		pconn.Flush()

		resp, err = rproxy.Parse(pconn.BufioReader())
		if err != nil {
			log.Error(err)
			pconnhash[host].PutConn(pconn)
			pmux.RUnlock()
			conn.Close()
			break
		}
		conn.SetWriteDeadline(time.Now().Add(time.Second*30))
		resp.WriteTo(conn)
		pconnhash[host].PutConn(pconn)
		pmux.RUnlock()
		//log.Info(111111)
	}
	//log.Error("closed")
}

func main() {
	go func() {
		t := time.NewTicker(time.Second * 200)
		for {
			select {
			case <-t.C:
				initPoolHash()
				log.Info(serverhost, len(pconnhash))
			}
		}
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
