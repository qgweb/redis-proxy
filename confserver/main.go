package main

import (
	"flag"
	"github.com/garyburd/redigo/redis"
	"strings"
	"time"
	"sync"
	"net/http"
	"encoding/json"
)

var (
	hosts = flag.String("hosts", "127.0.0.1:6379,127.0.0.1:6380", "默认redis地址")
	phost = flag.String("host", "127.0.0.1", "监听地址")
	pport = flag.String("port", "13457", "监听端口")
	hostMap map[string]*redis.Pool
	mux sync.RWMutex
	okhosts string
	statshosts string
)

func init() {
	flag.Parse()
	mux = sync.RWMutex{}
	hostMap = make(map[string]*redis.Pool)
	hostsAry := strings.Split(*hosts, ",")
	okhosts = *hosts
	for _, host := range hostsAry {
		hostMap[host] = func(h string) *redis.Pool {
			return &redis.Pool{
				MaxIdle: 10,
				IdleTimeout: 240 * time.Second,
				Dial: func() (redis.Conn, error) {
					c, err := redis.Dial("tcp", h)
					if err != nil {
						return nil, err
					}
					return c, err
				},
				TestOnBorrow: func(c redis.Conn, t time.Time) error {
					_, err := c.Do("PING")
					return err
				},
			}
		}(host)
	}
}

func pinghosts() {
	mux.Lock()
	defer mux.Unlock()
	var hosts = make([]string, 0, len(hostMap))
	var shosts = make(map[string][]string)
	for k, p := range hostMap {
		conn := p.Get()
		str, err := redis.String(conn.Do("PING"))
		if err == nil && str == "PONG" {
			hosts = append(hosts, k)
		}
		str, err = redis.String(conn.Do("INFO"))
		if err == nil && str != "" {
			shosts[k] = strings.Split(str,"\n")
		}
		conn.Close()
	}
	if sbtye, err := json.Marshal(shosts); err == nil {
		statshosts = string(sbtye)
	}
	okhosts = strings.Join(hosts, ",")
}

func main() {
	go func() {
		t := time.NewTicker(time.Second * 10)
		for {
			select {
			case <-t.C:
				pinghosts()
			}
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mux.RLock()
		w.Write([]byte(okhosts))
		mux.RUnlock()
	})
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		mux.RLock()
		w.Write([]byte(statshosts))
		mux.RUnlock()
	})
	http.ListenAndServe(*phost + ":" + *pport, nil)

}
