package main

import (
	"sort"
	"time"

	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/nodeWatcher"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/timer"
)

func (xb *XBot) HandleImproveTimer(t timer.Timer) {
	xb.logger.Info("Improve timer trigger")
	if xb.activeView.isFull() {
		candidates := xb.passiveView.getRandomElementsFromView(xb.conf.PSL)
		for _, c := range candidates {
			_, alreadyExists := xb.pendingOptimizations[c.String()]
			if alreadyExists {
				continue
			}
			xb.measureNode(c)
			xb.pendingOptimizations[c.String()] = true
			xb.logger.Infof("measuring peer: %sß for improvement procedure", c.String())
		}
	}

	// todo iterate over nodes measured
}

func (xb *XBot) handlePeerMeasuredNotification(n notification.Notification) {
	peerMeasuredNotification := n.(PeerMeasuredNotification)
	peerMeasured := peerMeasuredNotification.peerMeasured
	peerMeasuredNInfo, err := xb.nodeWatcher.GetNodeInfo(peerMeasured)
	defer xb.nodeWatcher.Unwatch(peerMeasured, xb.ID())
	if err != nil {
		xb.logger.Errorf("peer was %s not being measured", peerMeasured.String())
		return
	}
	xb.logger.Infof("Peer measured: %s:%+v", peerMeasured.String(), peerMeasuredNInfo.LatencyCalc().CurrValue())
	// measurements such that active peers have known costs
	measuredScore := peerMeasuredNInfo.LatencyCalc().CurrValue().Milliseconds()
	if _, ok := xb.pendingActiveViewMeasurements[peerMeasured.String()]; ok {
		xb.logger.Infof("Measured peer %s is an active view measurement ", peerMeasured.String())
		if activeViewPeer, ok := xb.activeView.get(peerMeasured); ok {
			activeViewPeer.measuredScore = measuredScore
		}
		delete(xb.pendingActiveViewMeasurements, peerMeasured.String())
		return
	}

	// measurements for pending optimizations
	_, ok := xb.pendingOptimizations[peerMeasured.String()]
	if ok {
		delete(xb.pendingOptimizations, peerMeasured.String())
		xb.logger.Infof("Measured peer %s is a pending optimization ", peerMeasured.String())
		if xb.activeView.isFull() {
			toCompareWith := xb.activeView.asArr[xb.conf.UN:]
			sort.Sort(toCompareWith)
			for _, curr := range toCompareWith {
				if xb.isBetter(curr.measuredScore, measuredScore) {
					// latency to curr is better
					xb.logger.Infof("Measured peer %s:%+v is better than peer %s:%+v!",
						peerMeasured.String(), measuredScore, curr.String(), curr.measuredScore)
					xb.sendMessageTmpTransport(OptimizationMessage{
						o: curr,
					}, peerMeasured)
					return
				}
				xb.logger.Infof("Measured peer %s:%+v is not better than peer %s:%+v",
					peerMeasured.String(), measuredScore, curr.String(), curr.measuredScore)
			}
		} else {
			xb.logger.Warn("Discarding measurement because active view is not full")
		}
	}

	// measurements for pending replacements
	replacement, ok := xb.pendingReplacements[peerMeasured.String()]
	if ok {
		delete(xb.pendingReplacements, peerMeasured.String())
		xb.logger.Infof("Measured peer %s is a pending replacement ", peerMeasured.String())
		sender, ok := xb.activeView.get(replacement.candidate)
		if !ok || !xb.isBetter(measuredScore, sender.measuredScore) {
			if !ok {
				xb.logger.Errorf("candidate peer is not in active view anymore")
			}

			if ok && !xb.isBetter(measuredScore, sender.measuredScore) {
				xb.logger.Errorf("candidate peer is not better")
			}

			xb.sendMessage(ReplaceMessageReply{
				answer:    false,
				initiator: replacement.initiator,
				o:         replacement.original,
			}, replacement.candidate)
			return
		}

		// is better!
		xb.sendMessage(SwitchMessage{
			i: replacement.initiator,
			c: replacement.candidate,
		}, peerMeasured)
	}
}

func (xb *XBot) HandleOptimizationMessage(sender peer.Peer, m message.Message) {
	optMsg := m.(OptimizationMessage)
	xb.logger.Infof("Got Optimization message %+v", optMsg)

	if !xb.activeView.isFull() {
		xb.addPeerToActiveView(sender)
		xb.sendMessageTmpTransport(OptimizationMessageReply{
			accepted:     true,
			hasOtherNode: false,
			d:            nil,
			o:            optMsg.o,
		}, sender)
		return
	}

	toCompareWith := xb.activeView.asArr[xb.conf.UN:]
	sort.Sort(toCompareWith)
	toDrop := toCompareWith[0]
	xb.sendMessageTmpTransport(ReplaceMessage{
		initiator: sender,
		o:         optMsg.o,
	}, toDrop)
}

func (xb *XBot) handleReplaceMsg(sender peer.Peer, m message.Message) {
	replaceMsg := m.(ReplaceMessage)
	xb.logger.Infof("Got Replace message %+v from %s", replaceMsg, sender)
	xb.pendingReplacements[replaceMsg.o.String()] = pendingReplacement{
		initiator: replaceMsg.initiator,
		original:  replaceMsg.o,
		candidate: sender,
	}
	xb.measureNode(replaceMsg.o)
	xb.logger.Infof("measuring peer: %s for replace procedure", replaceMsg.o.String())
}

func (xb *XBot) handleReplaceMsgReply(sender peer.Peer, m message.Message) {
	replaceMsgReply := m.(ReplaceMessageReply)
	xb.logger.Infof("Got Replace reply %+v from %s", replaceMsgReply, sender)
	if replaceMsgReply.answer {
		xb.activeView.remove(sender) // this is D
		xb.addPeerToActiveView(replaceMsgReply.initiator)
		xb.babel.SendMessageAndDisconnect(OptimizationMessageReply{
			accepted:     replaceMsgReply.answer,
			hasOtherNode: true,
			o:            replaceMsgReply.o,
			d:            sender,
		}, replaceMsgReply.initiator, xb.ID(), xb.ID())
		return
	}

	xb.sendMessage(OptimizationMessageReply{
		accepted:     replaceMsgReply.answer,
		d:            sender,
		o:            replaceMsgReply.o,
		hasOtherNode: false,
	}, replaceMsgReply.initiator)
}

func (xb *XBot) handleSwitchMsg(sender peer.Peer, m message.Message) {
	switchMsg := m.(SwitchMessage)
	xb.logger.Infof("Got switchMsg message %+v from %s", switchMsg, sender)
	initiator := switchMsg.i
	_, tombstone := xb.disconnectWaits[initiator.String()]
	accepted := false
	if xb.activeView.contains(initiator) || tombstone {
		xb.babel.SendMessageAndDisconnect(DisconnectWaitMessage{}, initiator, xb.ID(), xb.ID())
		delete(xb.disconnectWaits, initiator.String())
		xb.addPeerToActiveView(sender)
		accepted = true
	}
	xb.sendMessageTmpTransport(SwitchMessageReply{
		answer:    accepted,
		initiator: initiator,
		candidate: switchMsg.c,
	}, sender)
}

func (xb *XBot) handleSwitchMsgReply(sender peer.Peer, m message.Message) {
	switchMsgReply := m.(SwitchMessageReply)
	xb.logger.Infof("Got switchMsgReply message %+v from %s", switchMsgReply, sender)
	if switchMsgReply.answer {
		xb.activeView.remove(switchMsgReply.candidate)
		xb.babel.SendMessageAndDisconnect(ReplaceMessageReply{
			answer:    switchMsgReply.answer,
			initiator: switchMsgReply.initiator,
			o:         sender,
		}, switchMsgReply.candidate, xb.ID(), xb.ID())
		xb.addPeerToActiveView(sender)
		return
	}
	xb.sendMessage(ReplaceMessageReply{
		answer:    switchMsgReply.answer,
		initiator: switchMsgReply.initiator,
		o:         sender,
	}, switchMsgReply.candidate)
}

func (xb *XBot) handleOptimizationMsgReply(sender peer.Peer, m message.Message) {
	optMsgReply := m.(OptimizationMessageReply)
	xb.logger.Infof("Got OptimizationMessageReply %+v from %s", optMsgReply, sender)
	if optMsgReply.accepted {
		if xb.activeView.contains(optMsgReply.o) {
			if optMsgReply.hasOtherNode {
				xb.logger.Infof("Switching peer %s for %s", optMsgReply.o.String(), sender)
				xb.sendMessage(DisconnectWaitMessage{}, optMsgReply.o)
			} else {
				xb.sendMessage(DisconnectMessage{}, optMsgReply.o)
			}
			xb.activeView.remove(optMsgReply.o)
		}
		xb.passiveView.remove(sender)
		xb.addPeerToActiveView(sender)
	}
}

func (xb *XBot) handleDisconnectWaitMsg(sender peer.Peer, m message.Message) {
	xb.logger.Infof("Got DisconnectWait from %s", sender)
	xb.disconnectWaits[sender.String()] = true
	xb.babel.RegisterTimer(xb.ID(), DisconnectWaitTimeoutTimer{
		duration: 2 * time.Second, // TODO extract to constant
		peer:     sender,
	})
	xb.activeView.remove(sender)
	xb.addPeerToPassiveView(sender)
}

func (xb *XBot) isBetter(score1, score2 int64) bool {
	return score2-score1 > int64(xb.conf.MinImprovementForOptimizationMS)
}

func (xb *XBot) handleDisconnectWaitTimeoutTimer(t timer.Timer) {
	disconnectTimer := t.(DisconnectWaitTimeoutTimer)
	xb.logger.Warnf("disconnectWait timed out for peer %s", disconnectTimer.peer.String())
	delete(xb.disconnectWaits, disconnectTimer.peer.String())
}

func (xb *XBot) measureNode(p peer.Peer) {
	xb.logger.Infof("Measuring node %s", p.String())
	xb.nodeWatcher.Watch(p, xb.ID())
	condition := nodeWatcher.Condition{
		Repeatable:                false,
		CondFunc:                  func(nodeWatcher.NodeInfo) bool { return true },
		EvalConditionTickDuration: 500 * time.Millisecond,
		Notification:              NewPeerMeasuredNotification(p),
		Peer:                      p,
		EnableGracePeriod:         false,
		ProtoId:                   xb.ID(),
	}
	xb.nodeWatcher.NotifyOnCondition(condition)
}
