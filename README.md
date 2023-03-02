# omnipaxos-reconfiguration-service

## Ideas
### Process
 - OmniPaxos instances for each active config
    - When `<StopSign>` decided in config, periodically consult with leader to see when we can remove the config
 - Network layer handles to nodes
 - async runtime for handling messages to/from other nodes (also handle incoming requests from clients)

### Call reconfig
 - Propose `<StopSign>` (if new node calls reconfig maybe forward propose to leader?)
 - Add all data needed for reconfig to StopSign the metadata 
    - new PIDs
    - new network handles
    - custom transmission scheme
    - more?

### On decide `<StopSign>`
 - Update network handles
 - Create new OmniPaxos instance for new config
    - Do we need to transfer seq to new OmniPaxos instance like in lectures? (He doesn't do that in the [tutorial](https://haraldng.github.io/omnipaxos/omnipaxos/reconfiguration.html), also library cant append to a seq that contains a StopSign)

### TBD
 - Sequence transmission to new and old nodes


