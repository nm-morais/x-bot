package main

import (
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
)

const peerMeasuredNotificationID = 1000

type PeerMeasuredNotification struct {
	peerMeasured peer.Peer
}

func NewPeerMeasuredNotification(p peer.Peer) PeerMeasuredNotification {
	return PeerMeasuredNotification{
		peerMeasured: p,
	}
}

func (PeerMeasuredNotification) ID() notification.ID {
	return peerMeasuredNotificationID
}

const NeighborUpNotificationType = 10501

type NeighborUpNotification struct {
	PeerUp peer.Peer
}

func (n NeighborUpNotification) ID() notification.ID {
	return NeighborUpNotificationType
}

const NeighborDownNotificationType = 10502

type NeighborDownNotification struct {
	PeerDown peer.Peer
}

func (n NeighborDownNotification) ID() notification.ID {
	return NeighborDownNotificationType
}
