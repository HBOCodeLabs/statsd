package statsd

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/quipo/statsd/event"
)

// Logger interface compatible with log.Logger
type Logger interface {
	Println(v ...interface{})
}

// UDPPayloadSize is the number of bytes to send at one go through the udp socket.
// SendEvents will try to pack as many events into one udp packet.
// Change this value as per network capabilities
// For example to change to 16KB
//  import "github.com/quipo/statsd"
//  func init() {
//   statsd.UDPPayloadSize = 16 * 1024
//  }
var UDPPayloadSize = 512

// Hostname is exported so clients can set it to something different than the default
var Hostname string

var errNotConnected = fmt.Errorf("cannot send stats, not connected to StatsD server")

const defaultReconnectInterval = time.Second * 30

type socketType string

const (
	udpSocket socketType = "udp"
	tcpSocket socketType = "tcp"
)

func init() {
	host, err := os.Hostname()
	if nil == err {
		Hostname = host
	}
}

// StatsdClient is a client library to send events to StatsD
type StatsdClient struct {
	conn            net.Conn
	addr            string
	prefix          string
	eventStringTpl  string
	Logger          Logger
	connType        socketType
	reconnectTicker *time.Ticker
}

// ConfigurationFunc is a typedef for a function that configures some aspect of
// a StatsdClient instance
type ConfigurationFunc func(*StatsdClient)

// WithReconnectInterval returns a ConfigurationFunc that causes the StatsdClient to
// automatically recreate its underlying connection on the specified interval.
// If no reconnection interval is supplied via this configuration func, or the
// supplied interval is invalid, the default of 30 seconds will be used.
func WithReconnectInterval(interval time.Duration) ConfigurationFunc {
	return func(client *StatsdClient) {
		if interval > 0 {
			client.reconnectTicker = time.NewTicker(interval)
		}
	}
}

// WithLogger returns a ConfigurationFunc that makes the StatsdClient use the specified Logger
// implementation, rather than the default implementation.
func WithLogger(logger Logger) ConfigurationFunc {
	return func(client *StatsdClient) {
		client.Logger = logger
	}
}

// NewStatsdClient is a factory func that creates a StatsdClient that sends to
// the configured address and prefixes all stats with the given prefix name.
func NewStatsdClient(addr string, prefix string, options ...ConfigurationFunc) *StatsdClient {
	// allow %HOST% in the prefix string
	prefix = strings.Replace(prefix, "%HOST%", Hostname, 1)
	client := &StatsdClient{
		addr:           addr,
		prefix:         prefix,
		eventStringTpl: "%s%s:%s",
	}

	// apply all the configuration functions
	for _, opt := range options {
		opt(client)
	}

	// set some defaults, if necessary
	if client.Logger == nil {
		client.Logger = log.New(os.Stdout, "[StatsdClient] ", log.Ldate|log.Ltime)
	}

	if client.reconnectTicker == nil {
		client.reconnectTicker = time.NewTicker(defaultReconnectInterval)
	}

	go func() {
		for range client.reconnectTicker.C {
			err := client.Reconnect()
			if err != nil {
				client.Logger.Println(err)
			}
		}
	}()

	return client
}

// String returns the StatsD server address
func (c *StatsdClient) String() string {
	return c.addr
}

// Reconnect causes the client to re-create its underlying socket used to send
// stats. Any existing socket is closed after opening a new socket. Any error
// encountered while closing an existing socket is logged, but not returned.
func (c *StatsdClient) Reconnect() error {
	var err error
	prevSocket := c.conn

	switch c.connType {
	case udpSocket:
		c.Logger.Println("creating new udp socket")
		err = c.CreateSocket()
	case tcpSocket:
		c.Logger.Println("creating new tcp socket")
		err = c.CreateTCPSocket()
	default:
		return fmt.Errorf("no socket created, cannot identify connection type")
	}

	// The socket re-creation was successful.  Close any existing socket that previously existed
	if err == nil && prevSocket != nil {
		if e := prevSocket.Close(); e != nil {
			c.Logger.Println(fmt.Sprintf("error closing existing %s socket:", c.connType), e.Error())
		}
	}

	return err
}

// CreateSocket creates a UDP connection to a StatsD server
func (c *StatsdClient) CreateSocket() error {
	// Set the conn/socket type before dialing, so that if the socket creation
	// fails, the Reconnect method will still know what type of socket to
	// reconnect.
	c.connType = udpSocket
	conn, err := net.DialTimeout("udp", c.addr, 5*time.Second)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

// CreateTCPSocket creates a TCP connection to a StatsD server
func (c *StatsdClient) CreateTCPSocket() error {
	// Set the conn/socket type before dialing, so that if the socket creation
	// fails, the Reconnect method will still know what type of socket to
	// reconnect.
	c.connType = tcpSocket
	conn, err := net.DialTimeout("tcp", c.addr, 5*time.Second)
	if err != nil {
		return err
	}
	c.conn = conn
	c.eventStringTpl = "%s%s:%s\n"
	return nil
}

// Close the UDP connection
func (c *StatsdClient) Close() error {
	if nil == c.conn {
		return nil
	}
	return c.conn.Close()
}

// See statsd data types here: http://statsd.readthedocs.org/en/latest/types.html
// or also https://github.com/b/statsd_spec

// Incr - Increment a counter metric. Often used to note a particular event
func (c *StatsdClient) Incr(stat string, count int64) error {
	if 0 != count {
		return c.send(stat, "%d|c", count)
	}
	return nil
}

// Decr - Decrement a counter metric. Often used to note a particular event
func (c *StatsdClient) Decr(stat string, count int64) error {
	if 0 != count {
		return c.send(stat, "%d|c", -count)
	}
	return nil
}

// Timing - Track a duration event
// the time delta must be given in milliseconds
func (c *StatsdClient) Timing(stat string, delta int64) error {
	return c.send(stat, "%d|ms", delta)
}

// PrecisionTiming - Track a duration event
// the time delta has to be a duration
func (c *StatsdClient) PrecisionTiming(stat string, delta time.Duration) error {
	return c.send(stat, "%.6f|ms", float64(delta)/float64(time.Millisecond))
}

// Gauge - Gauges are a constant data type. They are not subject to averaging,
// and they don’t change unless you change them. That is, once you set a gauge value,
// it will be a flat line on the graph until you change it again. If you specify
// delta to be true, that specifies that the gauge should be updated, not set. Due to the
// underlying protocol, you can't explicitly set a gauge to a negative number without
// first setting it to zero.
func (c *StatsdClient) Gauge(stat string, value int64) error {
	if value < 0 {
		c.send(stat, "%d|g", 0)
		return c.send(stat, "%d|g", value)
	}
	return c.send(stat, "%d|g", value)
}

// GaugeDelta -- Send a change for a gauge
func (c *StatsdClient) GaugeDelta(stat string, value int64) error {
	// Gauge Deltas are always sent with a leading '+' or '-'. The '-' takes care of itself but the '+' must added by hand
	if value < 0 {
		return c.send(stat, "%d|g", value)
	}
	return c.send(stat, "+%d|g", value)
}

// FGauge -- Send a floating point value for a gauge
func (c *StatsdClient) FGauge(stat string, value float64) error {
	if value < 0 {
		c.send(stat, "%d|g", 0)
		return c.send(stat, "%g|g", value)
	}
	return c.send(stat, "%g|g", value)
}

// FGaugeDelta -- Send a floating point change for a gauge
func (c *StatsdClient) FGaugeDelta(stat string, value float64) error {
	if value < 0 {
		return c.send(stat, "%g|g", value)
	}
	return c.send(stat, "+%g|g", value)
}

// Absolute - Send absolute-valued metric (not averaged/aggregated)
func (c *StatsdClient) Absolute(stat string, value int64) error {
	return c.send(stat, "%d|a", value)
}

// FAbsolute - Send absolute-valued floating point metric (not averaged/aggregated)
func (c *StatsdClient) FAbsolute(stat string, value float64) error {
	return c.send(stat, "%g|a", value)
}

// Total - Send a metric that is continously increasing, e.g. read operations since boot
func (c *StatsdClient) Total(stat string, value int64) error {
	return c.send(stat, "%d|t", value)
}

// write a UDP packet with the statsd event
func (c *StatsdClient) send(stat string, format string, value interface{}) error {
	if c.conn == nil {
		return errNotConnected
	}
	stat = strings.Replace(stat, "%HOST%", Hostname, 1)
	// if sending tcp append a newline
	format = fmt.Sprintf(c.eventStringTpl, c.prefix, stat, format)
	_, err := fmt.Fprintf(c.conn, format, value)
	return err
}

// SendEvent - Sends stats from an event object
func (c *StatsdClient) SendEvent(e event.Event) error {
	if c.conn == nil {
		return errNotConnected
	}
	for _, stat := range e.Stats() {
		//fmt.Printf("SENDING EVENT %s%s\n", c.prefix, strings.Replace(stat, "%HOST%", Hostname, 1))
		_, err := fmt.Fprintf(c.conn, "%s%s", c.prefix, strings.Replace(stat, "%HOST%", Hostname, 1))
		if nil != err {
			return err
		}
	}
	return nil
}

// SendEvents - Sends stats from all the event objects.
// Tries to bundle many together into one fmt.Fprintf based on UDPPayloadSize.
func (c *StatsdClient) SendEvents(events map[string]event.Event) error {
	if c.conn == nil {
		return errNotConnected
	}

	var n int
	var stats = make([]string, 0)

	for _, e := range events {
		for _, stat := range e.Stats() {

			stat = fmt.Sprintf("%s%s", c.prefix, strings.Replace(stat, "%HOST%", Hostname, 1))
			_n := n + len(stat) + 1

			if _n > UDPPayloadSize {
				// with this last event, the UDP payload would be too big
				if _, err := fmt.Fprintf(c.conn, strings.Join(stats, "\n")); err != nil {
					return err
				}
				// reset payload after flushing, and add the last event
				stats = []string{stat}
				n = len(stat)
				continue
			}

			// can fit more into the current payload
			n = _n
			stats = append(stats, stat)
		}
	}

	if len(stats) != 0 {
		if _, err := fmt.Fprintf(c.conn, strings.Join(stats, "\n")); err != nil {
			return err
		}
	}

	return nil
}
