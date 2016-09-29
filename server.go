package rproxy

import (
	"stathat.com/c/consistent"
	"sync"
	"strings"
	"github.com/astaxie/beego/httplib"
	"github.com/juju/errors"
	"net"
	"time"
	"io"
	"bufio"
	"github.com/ngaut/log"
)

type RedisConn struct {
	rpool map[string]*Pool
	phash *consistent.Consistent
	mux   sync.RWMutex
}

func GetConfServer(conf string) ([]string, error) {
	resp := httplib.Get(conf)
	s, err := resp.String()
	if err != nil {
		return nil, err
	}
	return strings.Split(s, ","), nil
}

func NewRedisConn(rhosts []string) (*RedisConn, error) {
	if len(rhosts) == 0 {
		return nil, errors.New("redis 地址不能为空")
	}
	var rc = &RedisConn{}
	rc.phash = consistent.New()
	rc.phash.NumberOfReplicas = len(rhosts) * 20
	rc.rpool = make(map[string]*Pool)
	rc.phash.Set(rhosts)
	for _, host := range rhosts {
		rc.rpool[host] = NewPool(host, 1, func(addr string) (*Conn, error) {
			return NewConnection(addr, 30)
		})
	}
	return rc, nil
}

func (this *RedisConn) GetConn(key string) (*Conn, error) {
	this.mux.RLock()
	defer this.mux.RUnlock()

	host, err := this.phash.Get(key)
	if err != nil {
		return nil, err
	}
	if p, ok := this.rpool[host]; ok {
		return p.GetConn()
	}
	return nil, errors.New("无可用redis链接")
}

func (this *RedisConn) PutConn(key string, con *Conn) error {
	this.mux.RLock()
	defer this.mux.RUnlock()
	host, err := this.phash.Get(key)
	if err != nil {
		return err
	}
	if p, ok := this.rpool[host]; ok {
		p.PutConn(con)
		return nil
	} else {
		return errors.New("连接池无该地址:" + key)
	}
}

func (this *RedisConn) Reflush(rhosts []string) {
	this.mux.Lock()
	defer this.mux.Unlock()

	this.phash.Set(rhosts)
	for k := range this.rpool {
		found := false
		for _, v := range rhosts {
			if k == v {
				found = true
				break
			}
		}
		if !found {
			delete(this.rpool, k)
		}
	}
	for _, v := range rhosts {
		_, exists := this.rpool[v]
		if exists {
			continue
		}
		this.rpool[v] = NewPool(v, 10, func(addr string) (*Conn, error) {
			return NewConnection(addr, 30)
		})
	}
}

type RConnHandle struct {
	conn   net.Conn
	rconn  *RedisConn
	reader chan *Conn
	writer chan *Resp
	close  chan bool
}

func NewRConnHandle(conn net.Conn, rconn *RedisConn, wcount int) *RConnHandle {
	return &RConnHandle{conn, rconn, make(chan *Conn, wcount),
		make(chan *Resp, wcount), make(chan bool)}
}

func (this *RConnHandle) Reader() {
	bi := bufio.NewReader(this.conn)
	go this.ReadeProxy()
	go this.WriteProxy()
	go this.WriteProxy()
	go this.WriteProxy()

	var i float64
	for {
		resp, err := Parse(bi)
		i++
		//fmt.Println(i, err)
		if err != nil && err.Error() == io.EOF.Error() {
			this.conn.Close()
			this.close <- true
			break
		}
		if err != nil {
			log.Error(err)
			this.conn.Close()
			break
		}

		op, _, err := resp.GetOpKeys()
		if err != nil {
			log.Error(err)
			this.conn.Close()
			break
		}

		if strings.ToUpper(string(op)) == "PING" {
			this.conn.Write([]byte("+PONG\r\n"))
			continue
		}

		if strings.ToUpper(string(op)) == "INFO" {
			this.conn.Write([]byte("+OK\r\n"))
			continue
		}

		if strings.ToUpper(string(op)) == "QUIT" {
			this.conn.Close()
			break
		}

		this.writer <- resp

	}
	log.Info("close")
	this.close <- true
}

func (this *RConnHandle) WriteProxy() {
	for {
		select {
		case resp := <-this.writer:
			_, keys, _ := resp.GetOpKeys()
			kk := string(keys[0])
			pconn, err := this.rconn.GetConn(kk)
			if err != nil {
				log.Error(err)
				this.conn.Close()
				break
			}
			pconn.SetWriteDeadline(time.Now().Add(time.Second * 30))
			resp.WriteTo(pconn)
			pconn.Flush()
			this.rconn.PutConn(kk, pconn)
			this.reader <- pconn
		case <-this.close:
			log.Info(1)
			break
		}
	}
}

func (this *RConnHandle) ReadeProxy() {
	for {
		select {
		case pconn := <-this.reader:
			resp, err := Parse(pconn.BufioReader())
			if err != nil {
				log.Error(err)
				this.conn.Close()
				break
			}
			this.conn.SetWriteDeadline(time.Now().Add(time.Second * 30))
			resp.WriteTo(this.conn)
		case <-this.close:
			log.Info(1)
			break
		}
	}
}






