package statsd

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/quipo/statsd/event"
)

// MockNetConn is a mock for net.Conn
type MockNetConn struct {
	buf    bytes.Buffer
	Closed bool
}

func (mock *MockNetConn) Read(b []byte) (n int, err error) {
	return mock.buf.Read(b)
}

func (mock *MockNetConn) Write(b []byte) (n int, err error) {
	return mock.buf.Write(append(b, '\n'))
}

func (mock *MockNetConn) Close() error {
	mock.buf.Truncate(0)
	mock.Closed = true
	return nil
}

func (mock MockNetConn) LocalAddr() net.Addr {
	return nil
}

func (mock MockNetConn) RemoteAddr() net.Addr {
	return nil
}

func (mock MockNetConn) SetDeadline(t time.Time) error {
	return nil
}

func (mock MockNetConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (mock MockNetConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func newLocalListenerUDP(t *testing.T) (*net.UDPConn, *net.UDPAddr) {
	addr := fmt.Sprintf(":%d", getFreePort())
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	return ln, udpAddr
}

func TestTimingTCP(t *testing.T) {
	addr, ln := newLocalListenerTCP(t)
	defer ln.Close()

	prefix := "myproject."

	ch := make(chan string, 0)

	type metric struct {
		key string
		val int64
	}

	s := [4]metric{
		metric{"a:b:c", 5},
		metric{"d:e:f", 2},
		metric{"x:b:c", 5},
		metric{"g.h.i", 1},
	}

	expected := [4]string{
		"myproject.a:b:c:5|ms",
		"myproject.d:e:f:2|ms",
		"myproject.x:b:c:5|ms",
		"myproject.g.h.i:1|ms",
	}

	go doListenTCP(t, ln, ch, len(s))

	client := NewStatsdClient(addr, prefix)
	err := client.CreateTCPSocket()
	if nil != err {
		t.Fatal(err)
	}
	defer client.Close()

	for m := range s {
		client.Timing(s[m].key, s[m].val)
	}

	timeout := time.After(30 * time.Millisecond)
	for i := 0; i < len(s); i++ {
		select {
		case x := <-ch:
			if x != expected[i] {
				t.Fatal("")
			}
		case <-timeout:
			t.Fatal("Timed out")
		}
	}
}

func TestTotal(t *testing.T) {
	ln, udpAddr := newLocalListenerUDP(t)
	defer ln.Close()

	prefix := "myproject."

	client := NewStatsdClient(udpAddr.String(), prefix)

	ch := make(chan string, 0)

	s := map[string]int64{
		"a:b:c": 5,
		"d:e:f": 2,
		"x:b:c": 5,
		"g.h.i": 1,
	}

	expected := make(map[string]int64)
	for k, v := range s {
		expected[k] = v
	}

	// also test %HOST% replacement
	s["zz.%HOST%"] = 1
	hostname, err := os.Hostname()
	expected["zz."+hostname] = 1

	go doListenUDP(t, ln, ch, len(s))

	err = client.CreateSocket()
	if nil != err {
		t.Fatal(err)
	}
	defer client.Close()

	for k, v := range s {
		client.Total(k, v)
	}

	actual := make(map[string]int64)

	re := regexp.MustCompile(`^(.*)\:(\d+)\|(\w).*$`)

	for i := len(s); i > 0; i-- {
		x := <-ch
		x = strings.TrimSpace(x)
		//fmt.Println(x)
		if !strings.HasPrefix(x, prefix) {
			t.Errorf("Metric without expected prefix: expected '%s', actual '%s'", prefix, x)
			break
		}
		vv := re.FindStringSubmatch(x)
		if vv[3] != "t" {
			t.Errorf("Metric without expected suffix: expected 't', actual '%s'", vv[3])
		}
		v, err := strconv.ParseInt(vv[2], 10, 64)
		if err != nil {
			t.Error(err)
		}
		actual[vv[1][len(prefix):]] = v
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("did not receive all metrics: Expected: %T %v, Actual: %T %v ", expected, expected, actual, actual)
	}
}

func TestReconnecting(t *testing.T) {
	ln, udpAddr := newLocalListenerUDP(t)
	defer ln.Close()

	prefix := "test."

	client := NewStatsdClient(udpAddr.String(), prefix, WithReconnectInterval(10*time.Millisecond))

	ch := make(chan string, 0)

	s := map[string]int64{
		"a:b:c": 5,
		"d:e:f": 2,
	}

	go doListenUDP(t, ln, ch, len(s))

	client.CreateSocket()
	client.Close()

	time.Sleep(15 * time.Millisecond)
	for k, v := range s {
		fmt.Println("sent", k, v)
		client.Total(k, v)
	}

	timeout := time.After(30 * time.Millisecond)
	for i := len(s); i > 0; i-- {
		select {
		case x := <-ch:
			fmt.Println("received", x)
		case <-timeout:
			t.Fatal("Timed out")
		}
	}
}

func doListenUDP(t *testing.T, conn *net.UDPConn, ch chan string, n int) {
	for n > 0 {
		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go func(c *net.UDPConn, ch chan string) {
			buffer := make([]byte, 1024)
			size, err := c.Read(buffer)
			// size, address, err := sock.ReadFrom(buffer) <- This starts printing empty and nil values below immediatly
			if err != nil {
				fmt.Println(string(buffer), size, err)
				t.Fatal(err)
			}
			ch <- string(buffer)
		}(conn, ch)
		n--
	}
}

func doListenTCP(t *testing.T, conn net.Listener, ch chan string, n int) {
	client, err := conn.Accept()
	for {
		if err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 1024)
		c, err := client.Read(buf)
		if err != nil {
			if err.Error() == "EOF" {
				return
			}
			t.Fatal(err)
		}

		for _, s := range bytes.Split(buf[:c], []byte{'\n'}) {
			if len(s) > 0 {
				ch <- string(s)
			}
		}
	}
}

func newLocalListenerTCP(t *testing.T) (string, net.Listener) {
	addr := fmt.Sprintf("127.0.0.1:%d", getFreePort())
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return addr, ln
}

func TestTCP(t *testing.T) {
	addr, ln := newLocalListenerTCP(t)
	defer ln.Close()

	prefix := "myproject."
	client := NewStatsdClient(addr, prefix)

	ch := make(chan string, 0)

	s := map[string]int64{
		"a:b:c": 5,
		"d:e:f": 2,
		"x:b:c": 5,
		"g.h.i": 1,
	}

	expected := make(map[string]int64)
	for k, v := range s {
		expected[k] = v
	}

	// also test %HOST% replacement
	s["zz.%HOST%"] = 1
	hostname, err := os.Hostname()
	expected["zz."+hostname] = 1

	go doListenTCP(t, ln, ch, len(s))

	err = client.CreateTCPSocket()
	if nil != err {
		t.Fatal(err)
	}
	defer client.Close()

	for k, v := range s {
		client.Total(k, v)
	}

	actual := make(map[string]int64)

	re := regexp.MustCompile(`^(.*)\:(\d+)\|(\w).*$`)

	for i := len(s); i > 0; i-- {
		x := <-ch
		x = strings.TrimSpace(x)
		//fmt.Println(x)
		if !strings.HasPrefix(x, prefix) {
			t.Errorf("Metric without expected prefix: expected '%s', actual '%s'", prefix, x)
			break
		}
		vv := re.FindStringSubmatch(x)
		if vv[3] != "t" {
			t.Errorf("Metric without expected suffix: expected 't', actual '%s'", vv[3])
		}
		v, err := strconv.ParseInt(vv[2], 10, 64)
		if err != nil {
			t.Error(err)
		}
		actual[vv[1][len(prefix):]] = v
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("did not receive all metrics: Expected: %T %v, Actual: %T %v \n", expected, expected, actual, actual)
	}
}

func TestSocketCloseOnReconnect(t *testing.T) {
	c := NewStatsdClient("127.0.0.1:1201", "test")
	originalConn := &MockNetConn{} // mock connection
	c.conn = originalConn
	c.connType = udpSocket

	if originalConn.Closed {
		t.Errorf("expected socket not to be closed, but it was")
	}

	err := c.Reconnect()
	if err != nil {
		t.Fatal(err)
	}

	if !originalConn.Closed {
		t.Errorf("expected previous socket to be closed, but it wasn't")
	}
}

func TestSendEvents(t *testing.T) {
	c := NewStatsdClient("127.0.0.1:1201", "test")
	c.conn = &MockNetConn{} // mock connection

	// override with a small size
	UDPPayloadSize = 40

	e1 := &event.Increment{Name: "test1", Value: 123}
	e2 := &event.Increment{Name: "test2", Value: 432}
	e3 := &event.Increment{Name: "test3", Value: 111}
	e4 := &event.Gauge{Name: "test4", Value: 12435}

	events := map[string]event.Event{
		"test1": e1,
		"test2": e2,
		"test3": e3,
		"test4": e4,
	}

	err := c.SendEvents(events)
	if nil != err {
		t.Error(err)
	}

	b1 := make([]byte, UDPPayloadSize*3)
	n, err2 := c.conn.Read(b1)
	if nil != err2 {
		t.Error(err2)
	}
	nStats := len(strings.Split(strings.TrimSpace(string(b1[:n])), "\n"))
	if nStats != len(events) {
		t.Errorf("Was expecting %d events, got %d:  %s", len(events), nStats, string(b1))
	}
}

// getFreePort Ask the kernel for a free open port that is ready to use
func getFreePort() int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
