package cluster

type Watcher interface {
	NodeJoining(node *Node)
	NodeConnecting(node *Node)
	NodeOnline(node *Node)
	NodeDisconnecting(node *Node)
	NodeOffline(node *Node)
	NodeLeaving(node *Node)
	NodeLeaved(node *Node)

	//NodeJoiningRing(node *Node, nodeRing NodeRing, ring *Ring)
	//NodeJoinedRing(node *Node, nodeRing NodeRing, ring *Ring)
	//NodeLeavingRing(node *Node, nodeRing NodeRing, ring *Ring)
	//NodeLeavedRing(node *Node, nodeRing NodeRing, ring *Ring)
}

type WatcherNotifier struct {
	watchers []Watcher
}

func NewWatcherNotifier() *WatcherNotifier {
	wn := new(WatcherNotifier)
	return wn
}

func (wn *WatcherNotifier) Bind(watcher Watcher) {
	wn.watchers = append(wn.watchers, watcher)
}

func (wn *WatcherNotifier) NotifyNodeJoining(node *Node) {
	for _, watcher := range wn.watchers {
		watcher.NodeJoining(node)
	}
}

func (wn *WatcherNotifier) NotifyNodeConnecting(node *Node) {
	for _, watcher := range wn.watchers {
		watcher.NodeConnecting(node)
	}
}

func (wn *WatcherNotifier) NotifyNodeOnline(node *Node) {
	for _, watcher := range wn.watchers {
		watcher.NodeOnline(node)
	}
}

func (wn *WatcherNotifier) NotifyNodeDisconnecting(node *Node) {
	for _, watcher := range wn.watchers {
		watcher.NodeDisconnecting(node)
	}
}

func (wn *WatcherNotifier) NotifyNodeOffline(node *Node) {
}

func (wn *WatcherNotifier) NotifyNodeLeaving(node *Node) {
}

func (wn *WatcherNotifier) NotifyNodeLeaved(node *Node) {
}
