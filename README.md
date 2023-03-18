# omnipaxos-reconfiguration-service

Our project is an implementation of the project described in the project info as "2.2.1 Service layer for reconfiguration". We created a fault-tolerant server which maintains an Omnipaxos log with a cluster of similar servers (nodes). The cluster is able to be reconfigured by adding or removing nodes from the cluster. We also created a bare bones client which serves to give commands to the server such as appending to the log and initiating a reconfiguration.

# How to run
The following commands assume that the repo root is your working directory.

## Running a server with debug info:
```console
RUST_LOG=error,omnipaxos_server=debug cargo run -p omnipaxos_server <server's-node-id>
```
Note that the first configuration of nodes require configuration files in the `/config/` folder to start. To create files resulting in a simple first configuration you can run `clean.sh`.

## Running client requests:
```console
cargo run -p omnipaxos_client append 1 2 a 55
```
Sends a request to node with id `1` to append `KeyValue { key=a value=55}` to configuration with id `2`.

```console
cargo run -p omnipaxos_client reconfig 5 1 2 3
```
Sends a request to node with id `5` to propose a reconfiguration with a new cluster consisting of only nodes with id `1`, `2`, and `3`.
Note: only nodes 1-5 have IP addresses defined (in both the server and client's main.rs).