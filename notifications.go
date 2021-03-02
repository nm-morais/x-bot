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
