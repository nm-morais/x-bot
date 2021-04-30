package protocol

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/sirupsen/logrus"
)

type PeerStateArr []*PeerState

func (s PeerStateArr) Len() int {
	return len(s)
}
func (s PeerStateArr) Less(i, j int) bool {
	return s[i].measuredScore > s[j].measuredScore
}
func (s PeerStateArr) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type View struct {
	capacity int
	asArr    PeerStateArr
	asMap    map[string]*PeerState
	logger   *logrus.Logger
}

func (v *View) size() int {
	return len(v.asArr)
}

func (v *View) contains(p fmt.Stringer) bool {
	_, ok := v.asMap[p.String()]
	return ok
}

func (v *View) dropRandom() *PeerState {
	toDropIdx := getRandInt(len(v.asArr))
	peerDropped := v.asArr[toDropIdx]
	v.asArr = append(v.asArr[:toDropIdx], v.asArr[toDropIdx+1:]...)
	delete(v.asMap, peerDropped.String())
	return peerDropped
}

func (v *View) add(p *PeerState, dropIfFull bool) {
	v.logger.Infof("Adding peer %+v to view", p)

	if v.isFull() {
		if dropIfFull {
			v.dropRandom()
		} else {
			panic("adding peer to view already full")
		}
	}

	_, alreadyExists := v.asMap[p.String()]
	if !alreadyExists {
		v.asMap[p.String()] = p
		v.asArr = append([]*PeerState{p}, v.asArr...)
	} else {
		v.logger.Infof("Peer %+v already existed in view", p)
	}
}

func (v *View) remove(p peer.Peer) *PeerState {
	v.logger.Infof("Removing peer %+v from view", p)
	state, existed := v.asMap[p.String()]
	if existed {
		found := false
		for idx, curr := range v.asArr {
			if peer.PeersEqual(curr, p) {
				v.asArr = append(v.asArr[:idx], v.asArr[idx+1:]...)
				found = true
				break
			}
		}
		if !found {
			panic("node was in keys but not in array")
		}
		delete(v.asMap, p.String())
	} else {
		v.logger.Infof("Peer %+v did not exist in view", p)
	}
	return state
}

func (v *View) get(p fmt.Stringer) (*PeerState, bool) {
	elem, exists := v.asMap[p.String()]
	if !exists {
		return nil, false
	}
	return elem, exists
}

func (v *View) isFull() bool {
	return len(v.asArr) >= v.capacity
}

func (v *View) toArray() []*PeerState {
	return v.asArr
}

func (v *View) getRandomElementsFromView(amount int, exclusions ...peer.Peer) []peer.Peer {
	viewAsArr := v.toArray()
	perm := rand.Perm(len(viewAsArr))
	rndElements := []peer.Peer{}
	for i := 0; i < len(viewAsArr) && len(rndElements) < amount; i++ {
		excluded := false
		curr := viewAsArr[perm[i]]
		for _, exclusion := range exclusions {
			if peer.PeersEqual(exclusion, curr) {
				excluded = true
				break
			}
		}
		if !excluded {
			rndElements = append(rndElements, curr)
		}
	}
	return rndElements
}

type PeerState struct {
	peer.Peer
	outConnected  bool
	measuredScore int64
}
type pendingReplacement struct {
	initiator peer.Peer
	original  peer.Peer // o in scheme
	candidate peer.Peer
}

type XBotState struct {
	disconnectWaits               map[string]bool
	pendingActiveViewMeasurements map[string]bool
	pendingOptimizations          map[string]bool
	pendingReplacements           map[string]pendingReplacement
	activeView                    *View
	passiveView                   *View
}

func (xb *XBot) addPeerToActiveView(newPeer peer.Peer) {
	if peer.PeersEqual(xb.babel.SelfPeer(), newPeer) {
		xb.logger.Panic("Trying to add self to active view")
	}

	if xb.activeView.contains(newPeer) {
		xb.logger.Warnf("trying to add node %s already in active view", newPeer.String())
		return
	}

	if xb.activeView.isFull() {
		xb.dropRandomElemFromActiveView()
	}

	if xb.passiveView.contains(newPeer) {
		xb.passiveView.remove(newPeer)
		xb.logger.Infof("Removed node %s from passive view", newPeer.String())
	}

	xb.logger.Infof("Added peer %s to active view", newPeer.String())
	xb.activeView.add(&PeerState{
		Peer:          newPeer,
		outConnected:  false,
		measuredScore: math.MaxInt64,
	}, false)
	xb.babel.Dial(xb.ID(), newPeer, newPeer.ToTCPAddr())
	xb.pendingActiveViewMeasurements[newPeer.String()] = true
	xb.measureNode(newPeer)
	xb.logXBotState()
}

func (xb *XBot) addPeerToPassiveView(newPeer peer.Peer) {
	if peer.PeersEqual(newPeer, xb.babel.SelfPeer()) {
		xb.logger.Panic("trying to add self to passive view ")
	}

	if xb.activeView.contains(newPeer) {
		xb.logger.Warn("Trying to add node to passive view which is in active view")
		return
	}

	xb.passiveView.add(&PeerState{
		Peer:         newPeer,
		outConnected: false,
	}, true)
	xb.logger.Infof("Added peer %s to passive view", newPeer.String())
	xb.logXBotState()
}

func (xb *XBot) dropRandomElemFromActiveView() {
	if xb.activeView.size() > xb.conf.UN {
		toDropIdx := getRandInt(xb.conf.UN) + xb.conf.UN
		peerToDrop := xb.activeView.asArr[toDropIdx]
		xb.dropPeerFromActiveView(peerToDrop)
		xb.logger.Infof("Dropping peer %s from closest nodes", peerToDrop.String())
		return
	}

	xb.logger.Info("Dropping random peer (including further ones)")
	removed := xb.activeView.dropRandom()
	if removed != nil {
		xb.dropPeerFromActiveView(removed)
	}
}

func (xb *XBot) dropPeerFromActiveView(p peer.Peer) {
	removed := xb.activeView.remove(p)
	if removed != nil {
		xb.addPeerToPassiveView(p)
		xb.logXBotState()
		disconnectMsg := DisconnectMessage{}
		if removed.outConnected {
			xb.babel.SendMessageAndDisconnect(disconnectMsg, removed, xb.ID(), xb.ID())
			xb.babel.SendNotification(NeighborDownNotification{PeerDown: removed})
		} else {
			xb.babel.SendMessageSideStream(disconnectMsg, removed, removed.ToTCPAddr(), xb.ID(), xb.ID())
		}
	}
}
