package protocol

import (
	"encoding/binary"

	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
)

const JoinMessageType = 2000

type JoinMessage struct{}
type joinMessageSerializer struct{}

var defaultJoinMessageSerializer = joinMessageSerializer{}

func (JoinMessage) Type() message.ID                                      { return JoinMessageType }
func (JoinMessage) Serializer() message.Serializer                        { return defaultJoinMessageSerializer }
func (JoinMessage) Deserializer() message.Deserializer                    { return defaultJoinMessageSerializer }
func (joinMessageSerializer) Serialize(msg message.Message) []byte        { return []byte{} }
func (joinMessageSerializer) Deserialize(msgBytes []byte) message.Message { return JoinMessage{} }

const DisconnectMessageType = 2001

type DisconnectMessage struct{}
type disconnectMessageSerializer struct{}

var defaultDisconnectMessageSerializer = disconnectMessageSerializer{}

func (DisconnectMessage) Type() message.ID               { return DisconnectMessageType }
func (DisconnectMessage) Serializer() message.Serializer { return defaultDisconnectMessageSerializer }
func (DisconnectMessage) Deserializer() message.Deserializer {
	return defaultDisconnectMessageSerializer
}
func (disconnectMessageSerializer) Serialize(msg message.Message) []byte { return []byte{} }
func (disconnectMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	return DisconnectMessage{}
}

const ForwardJoinMessageType = 2002

type ForwardJoinMessage struct {
	TTL            uint32
	OriginalSender peer.Peer
}
type forwardJoinMessageSerializer struct{}

var defaultForwardJoinMessageSerializer = forwardJoinMessageSerializer{}

func (ForwardJoinMessage) Type() message.ID               { return ForwardJoinMessageType }
func (ForwardJoinMessage) Serializer() message.Serializer { return defaultForwardJoinMessageSerializer }
func (ForwardJoinMessage) Deserializer() message.Deserializer {
	return defaultForwardJoinMessageSerializer
}
func (forwardJoinMessageSerializer) Serialize(msg message.Message) []byte {
	converted := msg.(ForwardJoinMessage)
	ttlBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(ttlBytes, converted.TTL)
	return append(ttlBytes, converted.OriginalSender.Marshal()...)
}

func (forwardJoinMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	ttl := binary.BigEndian.Uint32(msgBytes[0:4])
	p := &peer.IPeer{}
	p.Unmarshal(msgBytes[4:])
	return ForwardJoinMessage{
		TTL:            ttl,
		OriginalSender: p,
	}
}

const ForwardJoinMessageReplyType = 2003

type ForwardJoinMessageReply struct {
}
type forwardJoinMessageReplySerializer struct{}

var defaultForwardJoinMessageReplySerializer = forwardJoinMessageReplySerializer{}

func (ForwardJoinMessageReply) Type() message.ID { return ForwardJoinMessageReplyType }
func (ForwardJoinMessageReply) Serializer() message.Serializer {
	return defaultForwardJoinMessageReplySerializer
}
func (ForwardJoinMessageReply) Deserializer() message.Deserializer {
	return defaultForwardJoinMessageReplySerializer
}
func (forwardJoinMessageReplySerializer) Serialize(msg message.Message) []byte {
	return []byte{}
}

func (forwardJoinMessageReplySerializer) Deserialize(msgBytes []byte) message.Message {
	return ForwardJoinMessageReply{}
}

const NeighbourMessageType = 2004

type NeighbourMessage struct {
	HighPrio bool
}
type neighbourMessageSerializer struct{}

var defaultNeighbourMessageSerializer = neighbourMessageSerializer{}

func (NeighbourMessage) Type() message.ID               { return NeighbourMessageType }
func (NeighbourMessage) Serializer() message.Serializer { return defaultNeighbourMessageSerializer }
func (NeighbourMessage) Deserializer() message.Deserializer {
	return defaultNeighbourMessageSerializer
}
func (neighbourMessageSerializer) Serialize(msg message.Message) []byte {
	converted := msg.(NeighbourMessage)
	var msgBytes []byte
	if converted.HighPrio {
		msgBytes = []byte{1}
	} else {
		msgBytes = []byte{0}
	}
	return msgBytes
}

func (neighbourMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	highPrio := msgBytes[0] == 1
	return NeighbourMessage{
		HighPrio: highPrio,
	}
}

const NeighbourMessageReplyType = 2005

type NeighbourMessageReply struct {
	Accepted bool
}
type neighbourMessageReplySerializer struct{}

var defaultNeighbourMessageReplySerializer = neighbourMessageReplySerializer{}

func (NeighbourMessageReply) Type() message.ID { return NeighbourMessageReplyType }
func (NeighbourMessageReply) Serializer() message.Serializer {
	return defaultNeighbourMessageReplySerializer
}
func (NeighbourMessageReply) Deserializer() message.Deserializer {
	return defaultNeighbourMessageReplySerializer
}
func (neighbourMessageReplySerializer) Serialize(msg message.Message) []byte {
	converted := msg.(NeighbourMessageReply)
	var msgBytes []byte
	if converted.Accepted {
		msgBytes = []byte{1}
	} else {
		msgBytes = []byte{0}
	}
	return msgBytes
}

func (neighbourMessageReplySerializer) Deserialize(msgBytes []byte) message.Message {
	accepted := msgBytes[0] == 1
	return NeighbourMessageReply{
		Accepted: accepted,
	}
}

const ShuffleMessageType = 2006

type ShuffleMessage struct {
	ID    uint32
	TTL   uint32
	Peers []peer.Peer
}
type ShuffleMessageSerializer struct{}

var defaultShuffleMessageSerializer = ShuffleMessageSerializer{}

func (ShuffleMessage) Type() message.ID { return ShuffleMessageType }
func (ShuffleMessage) Serializer() message.Serializer {
	return defaultShuffleMessageSerializer
}
func (ShuffleMessage) Deserializer() message.Deserializer {
	return defaultShuffleMessageSerializer
}
func (ShuffleMessageSerializer) Serialize(msg message.Message) []byte {
	msgBytes := make([]byte, 8)
	shuffleMsg := msg.(ShuffleMessage)
	binary.BigEndian.PutUint32(msgBytes[0:4], shuffleMsg.ID)
	binary.BigEndian.PutUint32(msgBytes[4:8], shuffleMsg.TTL)
	return append(msgBytes, peer.SerializePeerArray(shuffleMsg.Peers)...)
}

func (ShuffleMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	id := binary.BigEndian.Uint32(msgBytes[0:4])
	ttl := binary.BigEndian.Uint32(msgBytes[4:8])
	_, hosts := peer.DeserializePeerArray(msgBytes[8:])
	return ShuffleMessage{
		ID:    id,
		TTL:   ttl,
		Peers: hosts,
	}
}

const ShuffleReplyMessageType = 2007

type ShuffleReplyMessage struct {
	ID    uint32
	Peers []peer.Peer
}
type ShuffleReplyMessageSerializer struct{}

var defaultShuffleReplyMessageSerializer = ShuffleReplyMessageSerializer{}

func (ShuffleReplyMessage) Type() message.ID { return ShuffleReplyMessageType }
func (ShuffleReplyMessage) Serializer() message.Serializer {
	return defaultShuffleReplyMessageSerializer
}
func (ShuffleReplyMessage) Deserializer() message.Deserializer {
	return defaultShuffleReplyMessageSerializer
}
func (ShuffleReplyMessageSerializer) Serialize(msg message.Message) []byte {
	msgBytes := make([]byte, 4)
	shuffleMsg := msg.(ShuffleReplyMessage)
	binary.BigEndian.PutUint32(msgBytes[0:4], shuffleMsg.ID)
	return append(msgBytes, peer.SerializePeerArray(shuffleMsg.Peers)...)
}

func (ShuffleReplyMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	id := binary.BigEndian.Uint32(msgBytes[0:4])
	_, hosts := peer.DeserializePeerArray(msgBytes[4:])
	return ShuffleReplyMessage{
		ID:    id,
		Peers: hosts,
	}
}

const OptimizationMessageType = 2008

type OptimizationMessage struct {
	O peer.Peer
}
type OptimizationMessageSerializer struct{}

var defaultOptimizationMessageSerializer = OptimizationMessageSerializer{}

func (*OptimizationMessage) Type() message.ID { return OptimizationMessageType }
func (*OptimizationMessage) Serializer() message.Serializer {
	return defaultOptimizationMessageSerializer
}
func (*OptimizationMessage) Deserializer() message.Deserializer {
	return defaultOptimizationMessageSerializer
}
func (OptimizationMessageSerializer) Serialize(msg message.Message) []byte {
	optMsg := msg.(*OptimizationMessage)
	msgBytes := optMsg.O.Marshal()
	return msgBytes
}

func (OptimizationMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	old := &peer.IPeer{}
	old.Unmarshal(msgBytes)
	return &OptimizationMessage{
		O: old,
	}
}

const OptimizationMessageReplyType = 2009

type OptimizationMessageReply struct {
	accepted     bool
	hasOtherNode bool
	O            peer.Peer
	D            peer.Peer
}
type OptimizationMessageReplySerializer struct{}

var defaultOptimizationMessageReplySerializer = OptimizationMessageReplySerializer{}

func (*OptimizationMessageReply) Type() message.ID { return OptimizationMessageReplyType }
func (*OptimizationMessageReply) Serializer() message.Serializer {
	return defaultOptimizationMessageReplySerializer
}
func (*OptimizationMessageReply) Deserializer() message.Deserializer {
	return defaultOptimizationMessageReplySerializer
}
func (OptimizationMessageReplySerializer) Serialize(msg message.Message) []byte {
	optReplyMsg := msg.(*OptimizationMessageReply)
	msgBytes := make([]byte, 1)
	if optReplyMsg.accepted {
		msgBytes[0] = 1
	}
	if optReplyMsg.hasOtherNode {
		msgBytes = append(msgBytes, 1)
		msgBytes = append(msgBytes, optReplyMsg.D.Marshal()...)
	} else {
		msgBytes = append(msgBytes, 0)
	}
	msgBytes = append(msgBytes, optReplyMsg.O.Marshal()...)
	return msgBytes
}

func (OptimizationMessageReplySerializer) Deserialize(msgBytes []byte) message.Message {
	d := &peer.IPeer{}
	var otherNode peer.Peer = nil
	hasOtherNode := false
	accepted := false
	curr := 0
	if msgBytes[curr] == 1 {
		accepted = true
	}
	curr++
	if msgBytes[curr] == 1 {
		hasOtherNode = true
		curr++
		otherNode = &peer.IPeer{}
		curr += otherNode.Unmarshal(msgBytes[curr:])
	} else {
		curr++
	}
	d.Unmarshal(msgBytes[curr:])
	return &OptimizationMessageReply{
		accepted:     accepted,
		hasOtherNode: hasOtherNode,
		O:            otherNode,
		D:            d,
	}
}

const ReplaceMessageType = 2010

type ReplaceMessage struct {
	Initiator peer.Peer
	O         peer.Peer // this is O in the scheme
}
type ReplaceMessageSerializer struct{}

var defaultReplaceMessageSerializer = ReplaceMessageSerializer{}

func (*ReplaceMessage) Type() message.ID { return ReplaceMessageType }
func (*ReplaceMessage) Serializer() message.Serializer {
	return defaultReplaceMessageSerializer
}
func (*ReplaceMessage) Deserializer() message.Deserializer {
	return defaultReplaceMessageSerializer
}
func (ReplaceMessageSerializer) Serialize(msg message.Message) []byte {
	msgBytes := []byte{}
	optReplyMsg := msg.(*ReplaceMessage)
	msgBytes = append(msgBytes, optReplyMsg.O.Marshal()...)
	msgBytes = append(msgBytes, optReplyMsg.Initiator.Marshal()...)
	return msgBytes
}

func (ReplaceMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	replacement := &peer.IPeer{}
	initiator := &peer.IPeer{}
	n := replacement.Unmarshal(msgBytes)
	initiator.Unmarshal(msgBytes[n:])
	return &ReplaceMessage{
		O:         replacement,
		Initiator: initiator,
	}
}

const ReplaceMessageReplyType = 2011

type ReplaceMessageReply struct {
	answer    bool
	Initiator peer.Peer
	O         peer.Peer
}

type ReplaceMessageReplySerializer struct{}

var defaultReplaceMessageReplySerializer = ReplaceMessageReplySerializer{}

func (*ReplaceMessageReply) Type() message.ID { return ReplaceMessageReplyType }
func (*ReplaceMessageReply) Serializer() message.Serializer {
	return defaultReplaceMessageReplySerializer
}
func (*ReplaceMessageReply) Deserializer() message.Deserializer {
	return defaultReplaceMessageReplySerializer
}
func (ReplaceMessageReplySerializer) Serialize(msg message.Message) []byte {
	msgBytes := []byte{}
	optReplyMsg := msg.(*ReplaceMessageReply)
	if optReplyMsg.answer {
		msgBytes = append(msgBytes, 1)
	} else {
		msgBytes = append(msgBytes, 0)
	}
	msgBytes = append(msgBytes, optReplyMsg.Initiator.Marshal()...)
	msgBytes = append(msgBytes, optReplyMsg.O.Marshal()...)
	return msgBytes
}

func (ReplaceMessageReplySerializer) Deserialize(msgBytes []byte) message.Message {
	initiator := &peer.IPeer{}
	o := &peer.IPeer{}
	curr := 0
	answer := msgBytes[curr] == 1
	curr++
	curr += initiator.Unmarshal(msgBytes[curr:])
	o.Unmarshal(msgBytes[curr:])
	return &ReplaceMessageReply{
		answer:    answer,
		Initiator: initiator,
		O:         o,
	}
}

const SwitchMessageType = 2012

type SwitchMessage struct {
	I peer.Peer
	C peer.Peer
}
type SwitchMessageSerializer struct{}

var defaultSwitchMessageSerializer = SwitchMessageSerializer{}

func (*SwitchMessage) Type() message.ID { return SwitchMessageType }
func (*SwitchMessage) Serializer() message.Serializer {
	return defaultSwitchMessageSerializer
}
func (*SwitchMessage) Deserializer() message.Deserializer {
	return defaultSwitchMessageSerializer
}
func (SwitchMessageSerializer) Serialize(msg message.Message) []byte {
	msgBytes := []byte{}
	optReplyMsg := msg.(*SwitchMessage)
	msgBytes = append(msgBytes, optReplyMsg.C.Marshal()...)
	msgBytes = append(msgBytes, optReplyMsg.I.Marshal()...)
	return msgBytes
}

func (SwitchMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	candidate := &peer.IPeer{}
	initiator := &peer.IPeer{}
	curr := 0

	curr += candidate.Unmarshal(msgBytes)
	initiator.Unmarshal(msgBytes[curr:])

	return &SwitchMessage{
		I: initiator,
		C: candidate,
	}
}

const SwitchMessageReplyType = 2013

type SwitchMessageReply struct {
	answer    bool
	Initiator peer.Peer
	Candidate peer.Peer
}
type SwitchMessageReplySerializer struct{}

var defaultSwitchMessageReplySerializer = SwitchMessageReplySerializer{}

func (*SwitchMessageReply) Type() message.ID { return SwitchMessageReplyType }
func (*SwitchMessageReply) Serializer() message.Serializer {
	return defaultSwitchMessageReplySerializer
}
func (*SwitchMessageReply) Deserializer() message.Deserializer {
	return defaultSwitchMessageReplySerializer
}
func (SwitchMessageReplySerializer) Serialize(msg message.Message) []byte {
	msgBytes := []byte{}
	optReplyMsg := msg.(*SwitchMessageReply)
	if optReplyMsg.answer {
		msgBytes = append(msgBytes, 1)
	} else {
		msgBytes = append(msgBytes, 0)
	}
	msgBytes = append(msgBytes, optReplyMsg.Initiator.Marshal()...)
	msgBytes = append(msgBytes, optReplyMsg.Candidate.Marshal()...)
	return msgBytes
}

func (SwitchMessageReplySerializer) Deserialize(msgBytes []byte) message.Message {
	initiator := &peer.IPeer{}
	candidate := &peer.IPeer{}
	answer := msgBytes[0] == 1
	curr := 1

	n := initiator.Unmarshal(msgBytes[curr:])
	curr += n
	candidate.Unmarshal(msgBytes[curr:])
	return &SwitchMessageReply{
		answer:    answer,
		Initiator: initiator,
		Candidate: candidate,
	}
}

const DisconnectWaitType = 2014

type DisconnectWaitMessage struct{}
type DisconnectWaitSerializer struct{}

var defaultDisconnectWaitSerializer = DisconnectWaitSerializer{}

func (*DisconnectWaitMessage) Type() message.ID { return DisconnectWaitType }
func (*DisconnectWaitMessage) Serializer() message.Serializer {
	return defaultDisconnectWaitSerializer
}
func (*DisconnectWaitMessage) Deserializer() message.Deserializer {
	return defaultDisconnectWaitSerializer
}
func (DisconnectWaitSerializer) Serialize(msg message.Message) []byte {
	return []byte{}
}

func (DisconnectWaitSerializer) Deserialize(msgBytes []byte) message.Message {
	return &DisconnectWaitMessage{}
}
