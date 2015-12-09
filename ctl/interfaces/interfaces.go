package interfaces

import (
	"errors"
)

var ErrorWrongRev = errors.New("wrong rev")

type Rev string

type URL string

type UUID string

type Node struct {
	UUID UUID // Cluster manager assigned opaque UUID for a service on a node.

	// The node’s service URL (e.g., REST endpoint of the service process).
	ServiceURL URL

	// The node’s local cluster-manager URL (e.g., ns-server’s local REST endpoint).
	ManagerURL URL

	// TBD: Other fields.
}

type ActivityMessage struct {
	DateTime string
	Message  string
}

type Activities struct {
	// Map of Node UUID -> resource name -> ActivityPct.
	//
	// The Node UUID may be "", when the activity is a cluster-wide activity.
	//
	// A resource name is a service specific string, and is allowed to be ""
	// when an activity is general (perhaps like node shutdown).  For cbft,
	// for example, the resource names might be full text index names.
	Activities map[UUID]map[string]ActivityPct

	// A subset of recent log or notification messages from the service.
	Logs []ActivityMessage
}

type ActivityPct struct {
	// Activity is a short, service-specific string like "compaction",
	// "build", "deletion".  Some cluster manager relevant
	// activities would include "topology-rebalance" and "topology-failover",
	// which the cluster manager can use to display topology change progress.
	Activity string

	// 1.0 means 100% complete.  A negative value means the Pct field
	// is not applicable or relevant for this Activity notification.
	Pct float32
}

type Topology struct {
	// Rev is a CAS opaque identifier.  Any change to any Topology
	// field (including the ChangeTopology field) will mean a Rev change.
	Rev Rev

	// MemberNodes lists what the service thinks are the currently wanted
	// or "confirmed in" service nodes in the system.
	// MemberNodes field can change during the midst of a topology change,
	// but it is service specific on when and how MemberNodes will change
	// and stabilize.
	MemberNodes []Node

	// ChangeTopologyErrors holds the warnings from the last topology change
	// of the service.  NOTE: If the service manager (i.e., Ctl) restarts,
	// it may "forget" its previous ChangeTopologyWarnings field value (as
	// perhaps it was only tracked in memory).
	ChangeTopologyWarnings map[string][]string

	// ChangeTopologyErrors holds the errors from the last topology change
	// of the service.  NOTE: If the service manager (i.e., Ctl) restarts,
	// it may "forget" its previous ChangeTopologyErrors field value (as
	// perhaps it was only tracked in memory).
	ChangeTopologyErrors []error

	// ChangeTopology will be non-nil when a service topology change
	// is in progress, and holds info on the current ChangeTopology request.
	ChangeTopology *ChangeTopology
}

type GetTopology struct{}

type ChangeTopology struct {
	Rev Rev // Works as CAS, so use the last Topology response’s Rev.

	// Use Mode of "failover-hard" for hard failover.
	// Use Mode of "failover-graceful" for graceful failover.
	// Use Mode of "rebalance" for rebalance-style, clean and safe topology change.
	Mode string

	// The MemberNodes are the service nodes that should remain in the
	// service cluster after the topology change is finished.
	// When Mode is a variant of failover, then there should not be any
	// new nodes added to the MemberNodes (only service node removal is
	// allowed on failover).
	MemberNodes []Node
}
