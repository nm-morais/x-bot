package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	babel "github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/ungerik/go-dry"
	"gopkg.in/yaml.v2"
)

var (
	randomPort          *bool
	randomAnalyticsPort *bool
	bootstraps          *string
	listenIP            *string
)

func main() {
	for i := 0; i < len(os.Args); i++ {
		fmt.Printf("arg %d: %s\n", i, os.Args[i])
	}

	randomPort = flag.Bool("rport", false, "choose random port")
	randomAnalyticsPort = flag.Bool("raport", false, "choose random analytics port")

	bootstraps = flag.String("bootstraps", "", "choose custom bootstrap nodes (space-separated ip:port list)")
	listenIP = flag.String("listenIP", "", "choose custom ip to listen to")

	flag.Parse()

	conf := readConfFile()

	if *randomPort {
		fmt.Println("Setting custom port")
		freePort, err := GetFreePort()
		if err != nil {
			panic(err)
		}
		conf.SelfPeer.Port = freePort
	}

	if *randomAnalyticsPort {
		fmt.Println("Setting custom analytics port")
		freePort, err := GetFreePort()
		if err != nil {
			panic(err)
		}
		conf.SelfPeer.AnalyticsPort = freePort
	}

	ParseBootstrapArg(bootstraps, conf)
	if listenIP != nil && *listenIP != "" {
		fmt.Printf("Have custom ip: %s ", *listenIP)
		conf.SelfPeer.Host = *listenIP
	} else {
		fmt.Printf("Do not have custom ip: %s ", *listenIP)
	}

	conf.LogFolder += fmt.Sprintf("%s:%d/", conf.SelfPeer.Host, conf.SelfPeer.Port)
	selfPeer := peer.NewPeer(net.ParseIP(conf.SelfPeer.Host), uint16(conf.SelfPeer.Port), uint16(conf.SelfPeer.AnalyticsPort))

	protoManagerConf := babel.Config{
		Silent:    false,
		LogFolder: conf.LogFolder,
		SmConf: babel.StreamManagerConf{
			BatchMaxSizeBytes: 2000,
			BatchTimeout:      time.Second,
			DialTimeout:       time.Millisecond * time.Duration(conf.DialTimeoutMiliseconds),
		},
		HandshakeTimeout: 8 * time.Second,
		Peer:             selfPeer,
	}

	nwConf := &babel.NodeWatcherConf{
		PrintLatencyToInterval:    10 * time.Second,
		EvalConditionTickDuration: 1500 * time.Millisecond,
		MaxRedials:                2,
		TcpTestTimeout:            10 * time.Second,
		UdpTestTimeout:            10 * time.Second,
		NrTestMessagesToSend:      1,
		NrMessagesWithoutWait:     3,
		NrTestMessagesToReceive:   1,
		HbTickDuration:            1000 * time.Millisecond,
		MinSamplesLatencyEstimate: 3,
		OldLatencyWeight:          0.75,
		NewLatencyWeight:          0.25,
		PhiThreshold:              8.0,
		WindowSize:                20,
		MinStdDeviation:           500 * time.Millisecond,
		AcceptableHbPause:         1500 * time.Millisecond,
		FirstHeartbeatEstimate:    1500 * time.Millisecond,

		AdvertiseListenAddr: selfPeer.ToTCPAddr().IP,
		ListenAddr:          selfPeer.ToTCPAddr().IP,
		ListenPort:          conf.SelfPeer.AnalyticsPort,
	}

	p := babel.NewProtoManager(protoManagerConf)
	nw := babel.NewNodeWatcher(
		*nwConf,
		p,
	)
	p.RegisterNodeWatcher(nw)
	p.RegisterListenAddr(&net.TCPAddr{IP: protoManagerConf.Peer.IP(), Port: int(protoManagerConf.Peer.ProtosPort())})
	p.RegisterListenAddr(&net.UDPAddr{IP: protoManagerConf.Peer.IP(), Port: int(protoManagerConf.Peer.ProtosPort())})
	p.RegisterProtocol(NewXBotProtocol(p, nw, conf))
	p.StartSync()
}

func readConfFile() *XBotConfig {
	configFileName := "config/exampleConfig.yml"
	envVars := dry.EnvironMap()
	customConfig, ok := envVars["config"]
	if ok {
		configFileName = customConfig
	}
	f, err := os.Open(configFileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	cfg := &XBotConfig{}
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(cfg)
	if err != nil {
		panic(err)
	}
	return cfg
}

func GetFreePort() (port int, err error) {
	var a *net.TCPAddr
	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
	return
}

func ParseBootstrapArg(arg *string, conf *XBotConfig) {
	if arg != nil && *arg != "" {
		bootstrapPeers := []struct {
			Port          int    `yaml:"port"`
			Host          string `yaml:"host"`
			AnalyticsPort int    `yaml:"analyticsPort"`
		}{}
		fmt.Println("Setting custom bootstrap nodes")
		for _, ipPortStr := range strings.Split(*arg, " ") {
			split := strings.Split(ipPortStr, ":")
			ip := split[0]

			// assume all peers are running in same port if port is not specified
			var portInt int = conf.SelfPeer.Port
			var analyticsPortInt int = conf.SelfPeer.AnalyticsPort

			if len(split) > 1 {
				portIntAux, err := strconv.ParseInt(split[1], 10, 32)
				if err != nil {
					panic(err)
				}
				portInt = int(portIntAux)
			}

			if len(split) > 2 {
				portIntAux, err := strconv.ParseInt(split[2], 10, 32)
				if err != nil {
					panic(err)
				}
				analyticsPortInt = int(portIntAux)
			}

			fmt.Println(ip)
			bootstrapPeers = append(bootstrapPeers, struct {
				Port          int    `yaml:"port"`
				Host          string `yaml:"host"`
				AnalyticsPort int    `yaml:"analyticsPort"`
			}{
				Port:          portInt,
				Host:          ip,
				AnalyticsPort: analyticsPortInt,
			})
		}
		conf.BootstrapPeers = bootstrapPeers
	}
}
