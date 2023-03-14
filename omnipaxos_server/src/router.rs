use futures::{SinkExt, Stream};
use serde_json::{json, Value};
use tokio::net::{TcpStream, TcpListener, TcpSocket};
use tokio_serde::{formats::{Json, Cbor}, Framed};
use tokio_util::codec::{Framed as CodecFramed, LengthDelimitedCodec};
use anyhow::{anyhow, Error};
use std::{pin::Pin, collections::HashMap, net::SocketAddr};
use std::task::{Context, Poll};

use omnipaxos_core::{util::NodeId, messages::Message};
use crate::message::{NodeMessage, ClientMessage, ServerMessage};
use crate::kv::{KVSnapshot, KeyValue};

use std::mem;
use log::*;

/*
type FramedServer = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    Value,
    Value,
    Json<Value, Value>,
>;
*/

type NodeConnection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    NodeMessage,
    NodeMessage,
    Cbor<NodeMessage, NodeMessage>,
>;


pub struct Router {
    listener: TcpListener,
    nodes: HashMap<NodeId, NodeConnection>,
    node_addresses: HashMap<NodeId, SocketAddr>,
    pending_nodes: Vec<NodeConnection>,
    buffer: Vec<NodeMessage>
}

impl Router {
    pub async fn new(addr: SocketAddr, addresses: HashMap<NodeId, SocketAddr>) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await?;

        Ok(Self {
            listener,
            nodes: HashMap::new(),
            node_addresses: addresses,
            pending_nodes: vec![],
            buffer: vec![]
        })
    }
    
    pub async fn send_message(&mut self, node: NodeId, msg: NodeMessage) -> Result<(), Error> {
        if let Some(connection) = self.nodes.get_mut(&node) { 
            connection.send(msg).await?;
        } else {
            // No connection to node so if heartbeat message try to reconnect then send
            if let NodeMessage::OmniPaxosMessage(omnipaxos_core::messages::Message::BLE(_)) = msg {
                debug!("Trying to reconnect to node");
                self.add_node(node).await?;
                self.nodes.get_mut(&node).unwrap().send(msg).await?; // TODO: Need to called
                                                                         // reconnected on
                                                                         // reconnects 
            }
            return Err(anyhow!("Node `{}` not connected!", node));
        }
        Ok(())
    }

    async fn add_node(&mut self, node: NodeId) -> Result<(), Error> {
        if let Some(address) = self.node_addresses.get(&node) {
            //let socket = TcpSocket::new_v4()?;
            //socket.bind(self.listener.local_addr()?)?;
            let tcp_stream = self.listener.connect(*address).await?; 
            let length_delimited = CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());
            let framed = Framed::new(length_delimited, Cbor::default());
            self.nodes.insert(node, framed);
            return Ok(());
        }
        Err(anyhow!("No known address for node {}", node))
    }
    
    fn add_connection(&mut self, socket: TcpStream, address: SocketAddr) -> Result<(), Error> {
        let node_id = self.node_addresses.iter().find_map(|(id, addr)| if *addr == address {Some(id)} else {None});
        if let Some(&id) = node_id {
            let length_delimited = CodecFramed::new(socket, LengthDelimitedCodec::new());
            let framed = Framed::new(length_delimited, Cbor::default());
            self.nodes.insert(id, framed);
            return Ok(());
        }
        Err(anyhow!("No known node for address {}", address))
    }
}

impl Stream for Router {
    type Item = Result<NodeMessage, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = &mut self.as_mut();


        if let Poll::Ready(val) = Pin::new(&mut self_mut.listener).poll_accept(cx) {
            match val {
                Ok((tcp_stream, socket_addr)) => {
                    match self_mut.add_connection(tcp_stream, socket_addr) {
                        Err(err) => error!("Error adding new connection:{:?}", err),
                        _ => debug!("CONNECTED TO {}", socket_addr),
                    }
                }
                Err(err) => {
                    error!("Error checking for new requests:{:?}", err);
                    return Poll::Ready(None); // End stream
                }
            }
        }

        /* 
        let mut new_pending = Vec::new();

        mem::swap(&mut self_mut.pending_nodes, &mut new_pending);

        for mut pending in new_pending.into_iter() {
            if let Poll::Ready(val) = Pin::new(&mut pending).poll_next(cx) {
                match val {
                    Some(Ok(NodeMessage::Hello(id))) => {
                        debug!("New Node connection from `{}`",id);
                        self_mut.buffer.push(NodeMessage::Hello(id));
                        self_mut.nodes.insert(id, pending);
                    }
                    Some(Ok(msg)) => {
                        warn!("Received unknown message during handshake:{:?}", msg);
                    }
                    Some(Err(err)) => {
                        error!("Error checking for new requests:{:?}", err);
                    }
                    None => (),
                }
            } else {
                self_mut.pending_nodes.push(pending);
            }
        }
        */


        let mut new_nodes = HashMap::new();

        mem::swap(&mut self_mut.nodes, &mut new_nodes);

        for (name, mut client) in new_nodes.into_iter() {
            match Pin::new(&mut client).poll_next(cx) {
                Poll::Ready(Some(Ok(val))) => {
                    trace!("Received message from node `{}`: {:?}", name, val);
                    self_mut.buffer.push(val);
                    self_mut.nodes.insert(name, client);
                }
                Poll::Ready(None) => {
                    //Finished
                    debug!("Node `{}` disconnecting", name);
                }
                Poll::Ready(Some(Err(err))) => {
                    //Error
                    error!("Error from node `{}`: {} Removing connection.", name, err);
                }
                Poll::Pending => {
                    self_mut.nodes.insert(name, client);
                }
            }
        }


        if let Some(val) = self_mut.buffer.pop() {
            if self_mut.buffer.len() > 0 {
                cx.waker().wake_by_ref();
            }
            return Poll::Ready(Some(Ok(val)));
        }

        return Poll::Pending;
    }
}
