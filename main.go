package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"syscall"
	"time"
)

const VERSION = "0.0.1"

type Destination struct {
	Address string
	Regex   *regexp.Regexp
	Replace []byte
}

type Packet struct {
	Key  []byte
	Body []byte
}

type Rule struct {
	In   string   `json:"in"`
	Out  string   `json:"out"`
	Tags []string `json:"tags"`
	in   *regexp.Regexp
}

type Config struct {
	Rules []*Rule `json:"rules"`
}

var packetRegexp = regexp.MustCompile("^([^:]+):(.*)$")

func parseMessage(data []byte) []*Packet {
	var output []*Packet
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(line) == 0 {
			continue
		}

		item := packetRegexp.FindSubmatch(line)
		if len(item) == 0 {
			continue
		}

		packet := &Packet{
			Key:  item[1],
			Body: item[2],
		}
		output = append(output, packet)
	}
	return output
}

func processLoop(dataCh chan []byte, destAddress string, cfg *Config) {
	conn, err := net.DialTimeout("udp", destAddress, time.Second)
	if err != nil {
		log.Fatalf("ERROR: UDP connection failed - %s", err)
	}

	for data := range dataCh {
		for _, packet := range applyRules(data, cfg) {
			log.Printf("out: %s", packet)
			_, err = conn.Write([]byte(packet))
			if err != nil {
				log.Printf("ERROR: writing to UDP socket - %s", err)
				conn.Close()
				// reconnect
				conn, err = net.DialTimeout("udp", destAddress, time.Second)
				if err != nil {
					log.Fatalf("ERROR: UDP connection failed - %s", err)
				}
			}
		}
	}
}

func applyRules(data []byte, cfg *Config) [][]byte {
	var packets [][]byte
	for _, p := range parseMessage(data) {
		for _, r := range cfg.Rules {
			if !r.in.Match(p.Key) {
				continue
			}
			key := r.in.ReplaceAll(p.Key, []byte(r.Out))
			var tags [][]byte
			for _, tag := range r.Tags {
				tags = append(tags, r.in.ReplaceAll(p.Key, []byte(tag)))
			}
			packets = append(packets, []byte(fmt.Sprintf("%s:%s|#%s", key, p.Body, bytes.Join(tags, []byte(",")))))
			break
		}
	}
	return packets
}

func udpListener(address string, dataCh chan []byte) {
	addr, _ := net.ResolveUDPAddr("udp", address)
	log.Printf("listening on %s", addr)
	listener, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("ERROR: ListenUDP - %s", err)
	}
	defer listener.Close()

	err = listener.SetReadBuffer(1024 * 1024)
	if err != nil {
		log.Printf("ERROR: SetReadBuffer - %s", err)
	}

	for {
		message := make([]byte, 512)
		n, remaddr, err := listener.ReadFromUDP(message)
		if err != nil {
			log.Printf("ERROR: reading UDP packet from %+v - %s", remaddr, err)
			continue
		}

		log.Printf("msg: %s (%d)", message[:n], n)
		dataCh <- message[:n]
	}
}

func main() {
	var (
		address     = flag.String("address", ":8126", "UDP listening address")
		destAddress = flag.String("destination-address", ":8125", "UDP destination address")
		config      = flag.String("config", "rules.cfg", "path to config file")
		version     = flag.Bool("version", false, "print version")
	)
	flag.Parse()

	if *version {
		fmt.Printf("ddstatsd v%s (built w/%s)\n", VERSION, runtime.Version())
		return
	}

	data, err := ioutil.ReadFile(*config)
	if err != nil {
		log.Fatalf("ERROR: failed to read config file %s - %s", *config, err)
	}

	var cfg Config
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		log.Fatalf("ERROR: failed to decode config file %s - %s", *config, err)
	}

	for _, rule := range cfg.Rules {
		rule.in = regexp.MustCompile(rule.In)
	}

	runtime.GOMAXPROCS(2)

	signalchan := make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGTERM)

	dataCh := make(chan []byte, 1000)
	go udpListener(*address, dataCh)
	processLoop(dataCh, *destAddress, &cfg)
}
