package protocol

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/timer"
)

const ShuffleTimerID = 2001

type ShuffleTimer struct {
	duration time.Duration
}

func (ShuffleTimer) ID() timer.ID {
	return ShuffleTimerID
}

func (s ShuffleTimer) Duration() time.Duration {
	return s.duration
}

const PromoteTimerID = 2002

type PromoteTimer struct {
	duration time.Duration
}

func (PromoteTimer) ID() timer.ID {
	return PromoteTimerID
}

func (s PromoteTimer) Duration() time.Duration {
	return s.duration
}

const ImproveTimerID = 2003

type ImproveTimer struct {
	duration time.Duration
}

func (ImproveTimer) ID() timer.ID {
	return ImproveTimerID
}

func (s ImproveTimer) Duration() time.Duration {
	return s.duration
}

const MaintenanceTimerID = 2004

type MaintenanceTimer struct {
	duration time.Duration
}

func (MaintenanceTimer) ID() timer.ID {
	return MaintenanceTimerID
}

func (s MaintenanceTimer) Duration() time.Duration {
	return s.duration
}

const DisconnectWaitTimeoutTimerID = 2005

type DisconnectWaitTimeoutTimer struct {
	duration time.Duration
	peer     peer.Peer
}

func (DisconnectWaitTimeoutTimer) ID() timer.ID {
	return DisconnectWaitTimeoutTimerID
}

func (s DisconnectWaitTimeoutTimer) Duration() time.Duration {
	return s.duration
}

const DebugTimerID = 2006

type DebugTimer struct {
	duration time.Duration
}

func (DebugTimer) ID() timer.ID {
	return DebugTimerID
}

func (s DebugTimer) Duration() time.Duration {
	return s.duration
}
