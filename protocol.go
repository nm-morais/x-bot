package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net"
	"reflect"
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/nodeWatcher"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const (
	protoID = 1000
	name    = "XBot"
)

type XBotConfig struct {
	SelfPeer struct {
		AnalyticsPort int    `yaml:"analyticsPort"`
		Port          int    `yaml:"port"`
		Host          string `yaml:"host"`
	} `yaml:"self"`
	BootstrapPeers []struct {
		Port          int    `yaml:"port"`
		Host          string `yaml:"host"`
		AnalyticsPort int    `yaml:"analyticsPort"`
	} `yaml:"bootstrapPeers"`

	DialTimeoutMiliseconds          int    `yaml:"dialTimeoutMiliseconds"`
	LogFolder                       string `yaml:"logFolder"`
	JoinTimeSeconds                 int    `yaml:"joinTimeSeconds"`
	ActiveViewSize                  int    `yaml:"activeViewSize"`
	PassiveViewSize                 int    `yaml:"passiveViewSize"`
	ARWL                            int    `yaml:"arwl"`
	PRWL                            int    `yaml:"pwrl"`
	Ka                              int    `yaml:"ka"`
	Kp                              int    `yaml:"kp"`
	MinShuffleTimerDurationSeconds  int    `yaml:"minShuffleTimerDurationSeconds"`
	PromoteTimerDurationSeconds     int    `yaml:"promoteTimerDurationSeconds"`
	ImproveTimerDurationSeconds     int    `yaml:"improveTimerDurationSeconds"`
	DebugTimerDurationSeconds       int    `yaml:"debugTimerDurationSeconds"`
	MinImprovementForOptimizationMS int    `yaml:"minImprovementMilliseconds"`
	PSL                             int    `yaml:"psl"`
	UN                              int    `yaml:"un"`
}
type XBot struct {
	babel           protocolManager.ProtocolManager
	nodeWatcher     nodeWatcher.NodeWatcher
	lastShuffleMsg  *ShuffleMessage
	timeStart       time.Time
	logger          *logrus.Logger
	conf            *XBotConfig
	selfIsBootstrap bool
	bootstrapNodes  []peer.Peer
	*XBotState
}

func NewXBotProtocol(babel protocolManager.ProtocolManager, nw nodeWatcher.NodeWatcher, conf *XBotConfig) protocol.Protocol {
	logger := logs.NewLogger(name)
	selfIsBootstrap := false
	bootstrapNodes := []peer.Peer{}
	for _, p := range conf.BootstrapPeers {
		boostrapNode := peer.NewPeer(net.ParseIP(p.Host), uint16(p.Port), uint16(p.AnalyticsPort))
		bootstrapNodes = append(bootstrapNodes, boostrapNode)
		if peer.PeersEqual(babel.SelfPeer(), boostrapNode) {
			selfIsBootstrap = true
		}
	}

	logger.Infof("Starting with selfPeer:= %+v", babel.SelfPeer())
	logger.Infof("Starting with bootstraps:= %+v", bootstrapNodes)
	logger.Infof("Starting with selfIsBootstrap:= %+v", selfIsBootstrap)

	return &XBot{
		babel:          babel,
		nodeWatcher:    nw,
		lastShuffleMsg: nil,
		timeStart:      time.Time{},
		logger:         logger,
		conf:           conf,

		bootstrapNodes:  bootstrapNodes,
		selfIsBootstrap: selfIsBootstrap,

		XBotState: &XBotState{
			disconnectWaits:               map[string]bool{},
			pendingActiveViewMeasurements: map[string]bool{},
			pendingOptimizations:          map[string]bool{},
			pendingReplacements:           map[string]pendingReplacement{},

			activeView: &View{
				capacity: conf.ActiveViewSize,
				asArr:    []*PeerState{},
				asMap:    map[string]*PeerState{},
				logger:   logger,
			},
			passiveView: &View{
				capacity: conf.PassiveViewSize,
				asArr:    []*PeerState{},
				asMap:    map[string]*PeerState{},
				logger:   logger,
			},
		},
	}
}

func (xb *XBot) ID() protocol.ID {
	return protoID
}

func (xb *XBot) Name() string {
	return name
}

func (xb *XBot) Logger() *logrus.Logger {
	return xb.logger
}

func (xb *XBot) Init() {
	xb.babel.RegisterTimerHandler(protoID, ShuffleTimerID, xb.HandleShuffleTimer)
	xb.babel.RegisterTimerHandler(protoID, PromoteTimerID, xb.HandlePromoteTimer)
	xb.babel.RegisterTimerHandler(protoID, DebugTimerID, xb.HandleDebugTimer)

	// X-BOT addition
	xb.babel.RegisterTimerHandler(protoID, DisconnectWaitTimeoutTimerID, xb.handleDisconnectWaitTimeoutTimer)
	xb.babel.RegisterTimerHandler(protoID, ImproveTimerID, xb.HandleImproveTimer)

	xb.babel.RegisterMessageHandler(protoID, JoinMessage{}, xb.HandleJoinMessage)
	xb.babel.RegisterMessageHandler(protoID, ForwardJoinMessage{}, xb.HandleForwardJoinMessage)
	xb.babel.RegisterMessageHandler(protoID, ForwardJoinMessageReply{}, xb.HandleForwardJoinMessageReply)
	xb.babel.RegisterMessageHandler(protoID, ShuffleMessage{}, xb.HandleShuffleMessage)
	xb.babel.RegisterMessageHandler(protoID, ShuffleReplyMessage{}, xb.HandleShuffleReplyMessage)
	xb.babel.RegisterMessageHandler(protoID, NeighbourMessage{}, xb.HandleNeighbourMessage)
	xb.babel.RegisterMessageHandler(protoID, NeighbourMessageReply{}, xb.HandleNeighbourReplyMessage)
	xb.babel.RegisterMessageHandler(protoID, DisconnectMessage{}, xb.HandleDisconnectMessage)

	// X-BOT additions
	xb.babel.RegisterMessageHandler(protoID, &OptimizationMessage{}, xb.HandleOptimizationMessage)
	xb.babel.RegisterMessageHandler(protoID, &ReplaceMessage{}, xb.handleReplaceMsg)
	xb.babel.RegisterMessageHandler(protoID, &ReplaceMessageReply{}, xb.handleReplaceMsgReply)
	xb.babel.RegisterMessageHandler(protoID, &SwitchMessage{}, xb.handleSwitchMsg)
	xb.babel.RegisterMessageHandler(protoID, &SwitchMessageReply{}, xb.handleSwitchMsgReply)
	xb.babel.RegisterMessageHandler(protoID, &OptimizationMessageReply{}, xb.handleOptimizationMsgReply)
	xb.babel.RegisterMessageHandler(protoID, &DisconnectWaitMessage{}, xb.handleDisconnectWaitMsg)

	xb.babel.RegisterNotificationHandler(protoID, PeerMeasuredNotification{}, xb.handlePeerMeasuredNotification)
}

func (xb *XBot) Start() {
	xb.logger.Infof("Starting with confs: %+v", xb.conf)
	xb.babel.RegisterTimer(xb.ID(), ShuffleTimer{duration: time.Duration(xb.conf.JoinTimeSeconds) * time.Second})
	xb.babel.RegisterPeriodicTimer(xb.ID(), PromoteTimer{duration: time.Duration(xb.conf.PromoteTimerDurationSeconds) * time.Second}, false)
	xb.babel.RegisterPeriodicTimer(xb.ID(), ImproveTimer{time.Duration(xb.conf.ImproveTimerDurationSeconds) * time.Second}, false)
	xb.babel.RegisterPeriodicTimer(xb.ID(), DebugTimer{time.Duration(xb.conf.DebugTimerDurationSeconds) * time.Second}, false)
	xb.joinOverlay()
}

func (xb *XBot) joinOverlay() {
	xb.logXBotState()
	if xb.selfIsBootstrap {
		return
	}
	toSend := JoinMessage{}
	xb.logger.Info("Joining overlay...")
	if len(xb.bootstrapNodes) == 0 {
		xb.logger.Panic("No nodes to join overlay...")
	}
	bootstrapNode := xb.bootstrapNodes[getRandInt(len(xb.bootstrapNodes))]
	xb.babel.SendMessageSideStream(toSend, bootstrapNode, bootstrapNode.ToTCPAddr(), protoID, protoID)
}

func (xb *XBot) InConnRequested(dialerProto protocol.ID, p peer.Peer) bool {
	if dialerProto != xb.ID() {
		xb.logger.Warnf("Denying connection from peer %+v", p)
		return false
	}

	return true
}

func (xb *XBot) OutConnDown(p peer.Peer) {
	xb.handleNodeDown(p)
	xb.logger.Errorf("Peer %s out connection went down", p.String())
}

func (xb *XBot) DialFailed(p peer.Peer) {
	xb.logger.Errorf("Failed to dial peer %s", p.String())
	xb.handleNodeDown(p)
}

func (xb *XBot) handleNodeDown(p peer.Peer) {
	defer xb.logXBotState()
	defer xb.nodeWatcher.Unwatch(p, xb.ID())
	if xb.activeView.remove(p) {
		if !xb.activeView.isFull() {
			if xb.passiveView.size() == 0 {
				if xb.activeView.size() == 0 {
					xb.joinOverlay()
				}
				return
			}
			newNeighbor := xb.passiveView.getRandomElementsFromView(1)
			xb.logger.Infof("replacing downed with node %s from passive view", newNeighbor[0].String())
			xb.sendMessageTmpTransport(NeighbourMessage{
				HighPrio: xb.activeView.size() <= 1, // TODO review this
			}, newNeighbor[0])
		}
	}
}

func (xb *XBot) DialSuccess(sourceProto protocol.ID, p peer.Peer) bool {
	if sourceProto != xb.ID() {
		return false
	}
	foundPeer, found := xb.activeView.get(p)
	if found {
		foundPeer.outConnected = true
		xb.logger.Info("Dialed node in active view")
		return true
	}
	xb.logger.Infof("Disconnecting connection from peer %+v because it is not in active view", p)
	xb.babel.SendMessageAndDisconnect(DisconnectMessage{}, p, xb.ID(), xb.ID())
	return true
}

func (xb *XBot) MessageDelivered(msg message.Message, p peer.Peer) {
	if msg.Type() == DisconnectMessageType {
		if xb.activeView.contains(p) {
			xb.logger.Warnf("Delivered disconnect message to %s but peer is in active view", p.String())
		}
		xb.babel.Disconnect(xb.ID(), p)
		xb.logger.Infof("Disconnecting from %s", p.String())
	}
	xb.logger.Infof("Message of type [%s] was delivered to %s", reflect.TypeOf(msg), p.String())
}

func (xb *XBot) MessageDeliveryErr(msg message.Message, p peer.Peer, err errors.Error) {
	xb.logger.Warnf("Message %s was not sent to %s because: %s", reflect.TypeOf(msg), p.String(), err.Reason())
	_, isNeighMsg := msg.(NeighbourMessage)
	if isNeighMsg {
		xb.passiveView.remove(p)
	}
}

// ---------------- Protocol handlers (messages) ----------------

func (xb *XBot) HandleJoinMessage(sender peer.Peer, msg message.Message) {
	xb.logger.Infof("Received join message from %s", sender)
	if xb.activeView.isFull() {
		xb.dropRandomElemFromActiveView()
	}
	toSend := ForwardJoinMessage{
		TTL:            uint32(xb.conf.ARWL),
		OriginalSender: sender,
	}
	xb.addPeerToActiveView(sender)
	xb.sendMessageTmpTransport(ForwardJoinMessageReply{}, sender)
	for _, neigh := range xb.activeView.asArr {
		if peer.PeersEqual(neigh, sender) {
			continue
		}

		if neigh.outConnected {
			xb.logger.Infof("Sending ForwardJoin (original=%s) message to: %s", sender.String(), neigh.String())
			xb.sendMessage(toSend, neigh)
		}
	}
}

func (xb *XBot) HandleForwardJoinMessage(sender peer.Peer, msg message.Message) {
	fwdJoinMsg := msg.(ForwardJoinMessage)
	xb.logger.Infof("Received forward join message with ttl = %d, originalSender=%s from %s",
		fwdJoinMsg.TTL,
		fwdJoinMsg.OriginalSender.String(),
		sender.String())

	if fwdJoinMsg.OriginalSender == xb.babel.SelfPeer() {
		xb.logger.Panic("Received forward join message sent by myself")
	}

	if fwdJoinMsg.TTL == 0 || xb.activeView.size() == 1 {
		if fwdJoinMsg.TTL == 0 {
			xb.logger.Infof("Accepting forwardJoin message from %s, fwdJoinMsg.TTL == 0", fwdJoinMsg.OriginalSender.String())
		}
		if xb.activeView.size() == 1 {
			xb.logger.Infof("Accepting forwardJoin message from %s, xb.activeView.size() == 1", fwdJoinMsg.OriginalSender.String())
		}
		xb.addPeerToActiveView(fwdJoinMsg.OriginalSender)
		xb.sendMessageTmpTransport(ForwardJoinMessageReply{}, fwdJoinMsg.OriginalSender)
		return
	}

	if fwdJoinMsg.TTL == uint32(xb.conf.PRWL) {
		xb.addPeerToPassiveView(fwdJoinMsg.OriginalSender)
	}

	rndSample := xb.activeView.getRandomElementsFromView(1, fwdJoinMsg.OriginalSender, sender)
	if len(rndSample) == 0 { // only know original sender, act as if join message
		xb.logger.Errorf("Cannot forward forwardJoin message, dialing %s", fwdJoinMsg.OriginalSender.String())
		xb.addPeerToActiveView(fwdJoinMsg.OriginalSender)
		xb.sendMessageTmpTransport(ForwardJoinMessageReply{}, fwdJoinMsg.OriginalSender)
		return
	}

	toSend := ForwardJoinMessage{
		TTL:            fwdJoinMsg.TTL - 1,
		OriginalSender: fwdJoinMsg.OriginalSender,
	}
	nodeToSendTo := rndSample[0]
	xb.logger.Infof(
		"Forwarding forwardJoin (original=%s) with TTL=%d message to : %s",
		fwdJoinMsg.OriginalSender.String(),
		toSend.TTL,
		nodeToSendTo.String(),
	)
	xb.sendMessage(toSend, nodeToSendTo)
}

func (xb *XBot) HandleForwardJoinMessageReply(sender peer.Peer, msg message.Message) {
	xb.logger.Infof("Received forward join message reply from: %s", sender.String())
	xb.addPeerToActiveView(sender)
}

func (xb *XBot) HandleNeighbourMessage(sender peer.Peer, msg message.Message) {
	neighborMsg := msg.(NeighbourMessage)
	xb.logger.Infof("Received neighbor message %+v", neighborMsg)

	if neighborMsg.HighPrio {
		xb.addPeerToActiveView(sender)
		reply := NeighbourMessageReply{
			Accepted: true,
		}
		xb.sendMessageTmpTransport(reply, sender)
		return
	}

	if xb.activeView.isFull() {
		reply := NeighbourMessageReply{
			Accepted: false,
		}
		xb.sendMessageTmpTransport(reply, sender)
		return
	}
	xb.addPeerToActiveView(sender)
	reply := NeighbourMessageReply{
		Accepted: true,
	}
	xb.sendMessageTmpTransport(reply, sender)
}

func (xb *XBot) HandleNeighbourReplyMessage(sender peer.Peer, msg message.Message) {
	xb.logger.Info("Received neighbor reply message")
	neighborReplyMsg := msg.(NeighbourMessageReply)
	if neighborReplyMsg.Accepted {
		xb.addPeerToActiveView(sender)
	}
}

func (xb *XBot) HandleShuffleMessage(sender peer.Peer, msg message.Message) {
	shuffleMsg := msg.(ShuffleMessage)
	if shuffleMsg.TTL > 0 {
		rndSample := xb.activeView.getRandomElementsFromView(1, sender)
		if len(rndSample) != 0 {
			toSend := ShuffleMessage{
				ID:    shuffleMsg.ID,
				TTL:   shuffleMsg.TTL - 1,
				Peers: shuffleMsg.Peers,
			}
			xb.logger.Debug("Forwarding shuffle message to :", rndSample[0].String())
			xb.sendMessage(toSend, rndSample[0])
			return
		}
	}

	//  TTL is 0 or have no nodes to forward to
	//  select random nr of hosts from passive view
	exclusions := append(shuffleMsg.Peers, xb.babel.SelfPeer(), sender)
	toSend := xb.passiveView.getRandomElementsFromView(len(shuffleMsg.Peers), exclusions...)
	xb.mergeShuffleMsgPeersWithPassiveView(shuffleMsg.Peers, toSend)
	reply := ShuffleReplyMessage{
		ID:    shuffleMsg.ID,
		Peers: toSend,
	}
	xb.sendMessageTmpTransport(reply, sender)
}

func (xb *XBot) HandleShuffleReplyMessage(sender peer.Peer, m message.Message) {
	shuffleReplyMsg := m.(ShuffleReplyMessage)
	xb.logger.Infof("Received shuffle reply message %+v", shuffleReplyMsg)
	peersToDiscardFirst := []peer.Peer{}
	if xb.lastShuffleMsg != nil {
		peersToDiscardFirst = append(peersToDiscardFirst, xb.lastShuffleMsg.Peers...)
	}
	xb.lastShuffleMsg = nil
	xb.mergeShuffleMsgPeersWithPassiveView(shuffleReplyMsg.Peers, peersToDiscardFirst)
}

func (xb *XBot) HandleDisconnectMessage(sender peer.Peer, m message.Message) {
	xb.logger.Infof("Got Disconnect message from %s", sender.String())
	iPeer := sender
	xb.activeView.remove(iPeer)
	xb.addPeerToPassiveView(iPeer)
}

// ---------------- Protocol handlers (timers) ----------------

func (xb *XBot) HandlePromoteTimer(t timer.Timer) {
	xb.logger.Info("Promote timer trigger")
	if time.Since(xb.timeStart) > time.Duration(xb.conf.JoinTimeSeconds)*time.Second {
		if xb.activeView.size() == 0 && xb.passiveView.size() == 0 {
			xb.joinOverlay()
			return
		}
		if !xb.activeView.isFull() && xb.passiveView.size() > 0 {
			xb.logger.Info("Promoting node from passive view to active view")
			newNeighbor := xb.passiveView.getRandomElementsFromView(1)
			xb.sendMessageTmpTransport(NeighbourMessage{
				HighPrio: xb.activeView.size() <= 1, // TODO review this
			}, newNeighbor[0])
		}
	}
}

func (xb *XBot) HandleDebugTimer(t timer.Timer) {
	xb.logInView()
}

func (xb *XBot) HandleShuffleTimer(t timer.Timer) {
	xb.logger.Info("Shuffle timer trigger")
	minShuffleDuration := time.Duration(xb.conf.MinShuffleTimerDurationSeconds) * time.Second

	// add jitter to emission of shuffle messages
	toWait := minShuffleDuration + time.Duration(float32(minShuffleDuration)*rand.Float32())
	xb.babel.RegisterTimer(xb.ID(), ShuffleTimer{duration: toWait})

	if xb.activeView.size() == 0 {
		xb.logger.Info("No nodes to send shuffle message message to")
		return
	}

	passiveViewRandomPeers := xb.passiveView.getRandomElementsFromView(xb.conf.Kp - 1)
	activeViewRandomPeers := xb.activeView.getRandomElementsFromView(xb.conf.Ka)
	peers := append(passiveViewRandomPeers, activeViewRandomPeers...)
	peers = append(peers, xb.babel.SelfPeer())
	toSend := ShuffleMessage{
		ID:    uint32(getRandInt(math.MaxUint32)),
		TTL:   uint32(xb.conf.PRWL),
		Peers: peers,
	}
	xb.lastShuffleMsg = &toSend
	rndNode := xb.activeView.getRandomElementsFromView(1)
	xb.logger.Info("Sending shuffle message to: ", rndNode[0].String())
	xb.sendMessage(toSend, rndNode[0])
}

// ---------------- Auxiliary functions ----------------

func (xb *XBot) logXBotState() {
	xb.logger.Info("------------- XBot state -------------")
	var toLog string
	toLog = "Active view : "
	for _, p := range xb.activeView.asArr {
		toLog += fmt.Sprintf("%s:%d, ", p.String(), p.measuredScore)
	}
	xb.logger.Info(toLog)
	toLog = "Passive view : "
	for _, p := range xb.passiveView.asArr {
		toLog += fmt.Sprintf("%s, ", p.String())
	}
	xb.logger.Info(toLog)
	xb.logger.Info("-------------------------------------------")
}

func (xb *XBot) logInView() {
	type viewWithLatencies []struct {
		IP      string `json:"ip,omitempty"`
		Latency int    `json:"latency,omitempty"`
	}
	toPrint := viewWithLatencies{}
	for _, p := range xb.activeView.asArr {
		toPrint = append(toPrint, struct {
			IP      string "json:\"ip,omitempty\""
			Latency int    "json:\"latency,omitempty\""
		}{
			IP:      p.IP().String(),
			Latency: int(p.measuredScore),
		})
	}
	res, err := json.Marshal(toPrint)
	if err != nil {
		panic(err)
	}
	xb.logger.Infof("<inView> %s", string(res))
}

func (xb *XBot) sendMessage(msg message.Message, target peer.Peer) {
	xb.babel.SendMessage(msg, target, xb.ID(), xb.ID(), false)
}

func (xb *XBot) sendMessageTmpTransport(msg message.Message, target peer.Peer) {
	xb.babel.SendMessageSideStream(msg, target, target.ToTCPAddr(), xb.ID(), xb.ID())
}

func (xb *XBot) mergeShuffleMsgPeersWithPassiveView(shuffleMsgPeers, peersToKickFirst []peer.Peer) {
	for _, receivedHost := range shuffleMsgPeers {
		if xb.babel.SelfPeer().String() == receivedHost.String() {
			continue
		}

		if xb.activeView.contains(receivedHost) || xb.passiveView.contains(receivedHost) {
			continue
		}

		if xb.passiveView.isFull() { // if passive view is not full, skip check and add directly
			removed := false
			for _, firstToKick := range peersToKickFirst {
				if xb.passiveView.remove(firstToKick) {
					removed = true
					break
				}
			}
			if !removed {
				xb.passiveView.dropRandom() // drop random element to make space
			}
		}
		xb.addPeerToPassiveView(receivedHost)
	}
}
