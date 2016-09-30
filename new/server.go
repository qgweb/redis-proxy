package main

import (
	"net"
	"fmt"
	"bufio"
	"io"
	"strconv"
	"errors"
	"stathat.com/c/consistent"
	"flag"
	"strings"
	"syscall"
	"os/signal"
	"os"
	_"net/http/pprof"
	"net/http"
)

var (
	errUnbalancedQuotes = &errProtocol{"unbalanced quotes in request"}
	errInvalidBulkLength = &errProtocol{"invalid bulk length"}
	errInvalidMultiBulkLength = &errProtocol{"invalid multibulk length"}
	errIncompleteCommand = errors.New("incomplete command")
	errTooMuchData = errors.New("too much data")
)

type errProtocol struct {
	msg string
}

func (err *errProtocol) Error() string {
	return "Protocol error: " + err.msg
}

// Conn represents a client connection
type Conn interface {
	// RemoteAddr returns the remote address of the client connection.
	RemoteAddr() string
	// Close closes the connection.
	Close() error
	// WriteRaw writes raw data to the client.
	WriteRaw(data []byte)
	// SetReadBuffer updates the buffer read size for the connection
	SetReadBuffer(bytes int)
	// ReadPipeline returns all commands in current pipeline, if any
	// The commands are removed from the pipeline.
	ReadPipeline() []Command
	// PeekPipeline returns all commands in current pipeline, if any.
	// The commands remain in the pipeline.
	PeekPipeline() []Command
}
// conn represents a client connection
type conn struct {
	conn   *net.TCPConn
	wr     *Writer
	rd     *Reader
	addr   string
	closed bool
	cmds   []Command
}

func (c *conn) Close() error {
	c.closed = true
	return c.conn.Close()
}
func (c *conn) SetReadBuffer(n int) {}

func (c *conn) WriteRaw(data []byte) {
	c.wr.WriteRaw(data)
}
func (c *conn) RemoteAddr() string {
	return c.addr
}
func (c *conn) ReadPipeline() []Command {
	cmds := c.cmds
	c.cmds = nil
	return cmds
}
func (c *conn) PeekPipeline() []Command {
	return c.cmds
}

// Command represent a command
type Command struct {
	// Raw is a encoded RESP message.
	Raw  []byte
	// Args is a series of arguments that make up the command.
	Args [][]byte
}

type Writer struct {
	w io.Writer
	b []byte
}

// NewWriter creates a new RESP writer.
func NewWriter(wr io.Writer) *Writer {
	return &Writer{
		w: wr,
		b: make([]byte, 0, 4096),
	}
}

// Flush writes all unflushed Write* calls to the underlying writer.
func (w *Writer) Flush() error {
	if _, err := w.w.Write(w.b); err != nil {
		return err
	}
	w.b = w.b[:0]
	return nil
}
func (w *Writer) WriteArray(count int) {
	w.b = append(w.b, '*')
	w.b = append(w.b, strconv.FormatInt(int64(count), 10)...)
	w.b = append(w.b, '\r', '\n')
}

// WriteBulk writes bulk bytes to the client.
func (w *Writer) WriteBulk(bulk []byte) {
	w.b = append(w.b, '$')
	w.b = append(w.b, strconv.FormatInt(int64(len(bulk)), 10)...)
	w.b = append(w.b, '\r', '\n')
	w.b = append(w.b, bulk...)
	w.b = append(w.b, '\r', '\n')
}
// WriteError writes an error to the client.
func (w *Writer) WriteError(msg string) {
	w.b = append(w.b, '-')
	w.b = append(w.b, msg...)
	w.b = append(w.b, '\r', '\n')
}

// WriteRaw writes raw data to the client.
func (w *Writer) WriteRaw(data []byte) {
	w.b = append(w.b, data...)
}
func (w *Writer) WriteString(msg string) {
	if msg == "OK" {
		w.b = append(w.b, '+', 'O', 'K', '\r', '\n')
	} else {
		w.b = append(w.b, '+')
		w.b = append(w.b, []byte(msg)...)
		w.b = append(w.b, '\r', '\n')
	}
}
// Reader represent a reader for RESP or telnet commands.
type Reader struct {
	rd    *bufio.Reader
	buf   []byte
	start int
	end   int
	cmds  []Command
}

// NewReader returns a command reader which will read RESP or telnet commands.
func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd:  bufio.NewReader(rd),
		buf: make([]byte, 4096),
	}
}

func parseInt(b []byte) (int, error) {
	// shortcut atoi for 0-99. fails for negative numbers.
	switch len(b) {
	case 1:
		if b[0] >= '0' && b[0] <= '9' {
			return int(b[0] - '0'), nil
		}
	case 2:
		if b[0] >= '0' && b[0] <= '9' && b[1] >= '0' && b[1] <= '9' {
			return int(b[0] - '0') * 10 + int(b[1] - '0'), nil
		}
	}
	// fallback to standard library
	n, err := strconv.ParseUint(string(b), 10, 64)
	return int(n), err
}

func (rd *Reader) readCommands(leftover *int) ([]Command, error) {
	var cmds []Command
	b := rd.buf[rd.start:rd.end]

	if len(b) > 0 {
		// we have data, yay!
		// but is this enough data for a complete command? or multiple?
		next:
		switch b[0] {
		default:
			// just a plain text command
			for i := 0; i < len(b); i++ {
				if b[i] == '\n' {
					var line []byte
					if i > 0 && b[i - 1] == '\r' {
						line = b[:i - 1]
					} else {
						line = b[:i]
					}
					var cmd Command
					var quote bool
					var escape bool
					outer:
					for {
						nline := make([]byte, 0, len(line))
						for i := 0; i < len(line); i++ {
							c := line[i]
							if !quote {
								if c == ' ' {
									if len(nline) > 0 {
										cmd.Args = append(cmd.Args, nline)
									}
									line = line[i + 1:]
									continue outer
								}
								if c == '"' {
									if i != 0 {
										return nil, errUnbalancedQuotes
									}
									quote = true
									line = line[i + 1:]
									continue outer
								}
							} else {
								if escape {
									escape = false
									switch c {
									case 'n':
										c = '\n'
									case 'r':
										c = '\r'
									case 't':
										c = '\t'
									}
								} else if c == '"' {
									quote = false
									cmd.Args = append(cmd.Args, nline)
									line = line[i + 1:]
									if len(line) > 0 && line[0] != ' ' {
										return nil, errUnbalancedQuotes
									}
									continue outer
								} else if c == '\\' {
									escape = true
									continue
								}
							}
							nline = append(nline, c)
						}
						if quote {
							return nil, errUnbalancedQuotes
						}
						if len(line) > 0 {
							cmd.Args = append(cmd.Args, line)
						}
						break
					}
					if len(cmd.Args) > 0 {
						// convert this to resp command syntax
						var wr Writer
						wr.WriteArray(len(cmd.Args))
						for i := range cmd.Args {
							wr.WriteBulk(cmd.Args[i])
							cmd.Args[i] = append([]byte(nil), cmd.Args[i]...)
						}
						cmd.Raw = wr.b
						cmds = append(cmds, cmd)
					}
					b = b[i + 1:]
					if len(b) > 0 {
						goto next
					} else {
						goto done
					}
				}
			}
		case '*':
			// resp formatted command
			marks := make([]int, 0, 16)
			outer2:
			for i := 1; i < len(b); i++ {
				if b[i] == '\n' {
					if b[i - 1] != '\r' {
						return nil, errInvalidMultiBulkLength
					}
					count, err := parseInt(b[1 : i - 1])
					if err != nil || count <= 0 {
						return nil, errInvalidMultiBulkLength
					}
					marks = marks[:0]
					for j := 0; j < count; j++ {
						// read bulk length
						i++
						if i < len(b) {
							if b[i] != '$' {
								return nil, &errProtocol{"expected '$', got '" + string(b[i]) + "'"}
							}
							si := i
							for ; i < len(b); i++ {
								if b[i] == '\n' {
									if b[i - 1] != '\r' {
										return nil, errInvalidBulkLength
									}
									size, err := parseInt(b[si + 1 : i - 1])
									if err != nil || size < 0 {
										return nil, errInvalidBulkLength
									}
									if i + size + 2 >= len(b) {
										// not ready
										break outer2
									}
									if b[i + size + 2] != '\n' ||
										b[i + size + 1] != '\r' {
										return nil, errInvalidBulkLength
									}
									i++
									marks = append(marks, i, i + size)
									i += size + 1
									break
								}
							}
						}
					}
					if len(marks) == count * 2 {
						var cmd Command
						if rd.rd != nil {
							// make a raw copy of the entire command when
							// there's a underlying reader.
							cmd.Raw = append([]byte(nil), b[:i + 1]...)
						} else {
							// just assign the slice
							cmd.Raw = b[:i + 1]
						}
						cmd.Args = make([][]byte, len(marks) / 2)
						// slice up the raw command into the args based on
						// the recorded marks.
						for h := 0; h < len(marks); h += 2 {
							cmd.Args[h / 2] = cmd.Raw[marks[h]:marks[h + 1]]
						}
						cmds = append(cmds, cmd)
						b = b[i + 1:]
						if len(b) > 0 {
							goto next
						} else {
							goto done
						}
					}
				}
			}
		}
		done:
		rd.start = rd.end - len(b)
	}
	if leftover != nil {
		*leftover = rd.end - rd.start
	}
	if len(cmds) > 0 {
		return cmds, nil
	}
	if rd.rd == nil {
		return nil, errIncompleteCommand
	}
	if rd.end == len(rd.buf) {
		// at the end of the buffer.
		if rd.start == rd.end {
			// rewind the to the beginning
			rd.start, rd.end = 0, 0
		} else {
			// must grow the buffer
			newbuf := make([]byte, len(rd.buf) * 2)
			copy(newbuf, rd.buf)
			rd.buf = newbuf
		}
	}
	n, err := rd.rd.Read(rd.buf[rd.end:])
	if err != nil {
		return nil, err
	}
	rd.end += n
	return rd.readCommands(leftover)
}

// ReadCommand reads the next command.
func (rd *Reader) ReadCommand() (Command, error) {
	if len(rd.cmds) > 0 {
		cmd := rd.cmds[0]
		rd.cmds = rd.cmds[1:]
		return cmd, nil
	}
	cmds, err := rd.readCommands(nil)
	if err != nil {
		return Command{}, err
	}
	rd.cmds = cmds
	return rd.ReadCommand()
}

// Parse parses a raw RESP message and returns a command.
func Parse(raw []byte) (Command, error) {
	rd := Reader{buf: raw, end: len(raw)}
	var leftover int
	cmds, err := rd.readCommands(&leftover)
	if err != nil {
		return Command{}, err
	}
	if leftover > 0 {
		return Command{}, errTooMuchData
	}
	return cmds[0], nil

}

type BufConn struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func handler(c *conn, servers []string) {
	cons := consistent.New()
	cons.Set(servers)
	cons.NumberOfReplicas = len(servers) * 20

	var redisConns = make(map[string]*BufConn)
	var newRedisConn = func(addr string) *BufConn {
		rconn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Println("FUCK", err)
			return nil
		}
		tconn := rconn.(*net.TCPConn)
		tcpc := &BufConn{
			tconn,
			bufio.NewReaderSize(tconn, 4096),
			bufio.NewWriterSize(tconn, 4096),
		}
		return tcpc
	}

	var buf = make([]byte, 4096)
	for {
		cmd, err := c.rd.ReadCommand()
		if err != nil {
			if err, ok := err.(*errProtocol); ok {
				c.wr.WriteError("ERR " + err.Error())
				c.wr.Flush()
			}
			break
		}

		var hashkey = ""
		if len(cmd.Args) >= 2 {
			hashkey = string(cmd.Args[1])
		}
		if len(cmd.Args) == 1 {
			hashkey = string(cmd.Args[0])
		}

		host, err := cons.Get(hashkey)
		if err != nil {
			c.wr.WriteError("ERR " + err.Error())
			c.wr.Flush()
			continue
		}

		if _, ok := redisConns[host]; !ok {
			redisConns[host] = newRedisConn(host)
			if redisConns[host] == nil {
				c.wr.WriteError("ERR " + "连接redis实例失败，地址：" + host)
				c.wr.Flush()
				delete(redisConns, host)
				continue
			}
		}

		redisConns[host].w.Write(cmd.Raw)
		redisConns[host].w.Flush()

		for {
			n, err := redisConns[host].r.Read(buf)
			if err != nil {
				c.wr.WriteError(err.Error())
				break
			}
			if n <= len(buf) {
				c.wr.WriteRaw(buf[:n])
			}
			if n < len(buf) {
				break
			}
		}

		c.wr.Flush()
	}

	for _, v := range redisConns {
		v.conn.Close()
	}
	c.Close()
}

var (
	host = flag.String("host", ":8081", "绑定的地址")
	pprofhost = flag.String("phost", ":8082", "pprof绑定的地址")
	servers = flag.String("server", ":6379|:6380", "redis实例地址，按|隔开")
	_ = flag.String("help", "", `
	如需重新初始化redis实例地址
	使用 export REDISHOSTS = :6379|:6380
	kill -HUP server进程号`)
	exitChan = make(chan os.Signal, 1)
)

func init() {
	flag.Parse()
}
func main() {
	go func() {
		signal.Notify(exitChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT) // , syscall.SIGSTOP
		for {
			s := <-exitChan
			switch s {
			case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT: // , syscall.SIGSTOP
				os.Exit(0)
				return
			case syscall.SIGHUP:
				fmt.Println(111)
				if s := os.Getenv("REDISHOSTS"); s != "" {
					fmt.Println(s)
					*servers = s
				}
			default:
				return
			}
		}
	}()

	go func() {
		http.ListenAndServe(*pprofhost, nil)
	}()

	n, err := net.Listen("tcp", *host)
	if err != nil {
		fmt.Println(err)
		return
	}
	for {
		tcpc, err := n.(*net.TCPListener).AcceptTCP()
		if err != nil {
			fmt.Println("ERROR", err)
			continue
		}
		c := &conn{conn: tcpc, addr: tcpc.RemoteAddr().String(),
			wr: NewWriter(tcpc), rd: NewReader(tcpc)}
		go handler(c, strings.Split(*servers, "|"))
	}
}
