# omnipaxos-reconfiguration-service

## Ideas
### Network Layer
 - Network layer handles to nodes
 - async runtime for handling messages to/from other nodes (also handle incoming requests from clients)

### Log migration
 - Service Layer (what we have to implement)
    - if `<StopSign>` is detected in a server
       - if the server is the leader
          - create new servers (OmniPaxos) if there are some in the new config
             - metadata for the new servers
                - a list of old servers from which they can pull the log (specfied in `<StopSign>` meta data)
                - the length of log with `<StopSign>` excluded
                - network things
          - on `LogPullDone` for all new servers
             - send `StartNewConfiguration` to all servers
       - for new servers
          - once created, send `LogPullRequest` to old servers which they can pull log from
             - **TBD**
                - how to pull in the most efficient ways
                   - pull a constant number of log from each servers per request?
                - what if some old servers is unresponsive?
          - after catching up, send `LogPullDone` to the leader
       - for all old servers (followers, leader, and new servers)
          - On `LogPullRequest`
             - send back log segment requested
          - On `StartNewConfiguration`
             - create a new OmniPaxos if self is included in the new configuration
 - Log Replication Layer (what library has already done)
    - omnipaxos instance can call `reconfigure()` where it create a `ReconfigurationRequest` 
       - if it's a leader, it will try to propose it 
       - if it's a follower, it will forward it to its leader
    - `ReconfigurationRequest`
       - a vector of all servers in the new configuration
       - cid
       - meta data
          - all new servers with a list of old servers from which they can pull the log
             - 2d array I guess

### Correctness of the proposed log migration scheme
 - cannot decide new entry inbetween configurations (where Raft can)
 - parrallel migration between servers

