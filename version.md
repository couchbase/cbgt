Version
=======

### Documentation

The cbgt.VERSION tracks persistence versioning of the schema/format of
persisted data and configuration. You *must* update the cbgt.VERSION if you
change what's stored in the Cfg such as the JSON/struct definitions or the planning
algorithms. If the persisted data/config format was unchanged, then the cbgt.VERSION
number should remain unchanged.

These version number checks prevent the older planners from operating/modifying on
the configs laid out by the newer version planners, as the config formats would
have got changed over the version increments.

### Few rules of thumb for version management

In a multi node cluster, the version bump happens only when all the nodes in the
cluster reaches at a homogeneous CBGT version. This effective version consistency
check is performed over a compatibility version check rest call with ns-server
along with a version check derived from nodeDefiniions as a fallback mechanism,
in case the former approach fails.

- Ctl (rebalance), Planner are the two routines responsible for bumping the version
  value in a cluster.
- All routines are supposed to use the CfgGetVersion api to retrieve the effective
  version in a cluster before attempting any version sensitive config operations.
- The planner should respect the input version number parameterised to it.
- The ImplVersion value embedded inside the index definitions, node Definitions and
  plan pindexes are also supposed to get incremented only after the effective
  compatible version in a cluster gets incremented.
