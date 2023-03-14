use futures::{SinkExt, Stream};
use serde_json::{json, Value};
use tokio::net::{TcpStream, TcpListener, TcpSocket};
use tokio_serde::{formats::{Json, Cbor}, Framed};
use tokio_util::codec::{Framed as CodecFramed, LengthDelimitedCodec};
use anyhow::{anyhow, Error};
use std::{pin::Pin, collections::{HashMap, VecDeque}, net::SocketAddr};
use std::task::{Context, Poll};

use omnipaxos_core::{util::NodeId, messages::Message};
use crate::message::{NodeMessage, ClientMessage, ServerMessage};
use crate::kv::{KVSnapshot, KeyValue};


use std::mem;
use log::*;

type NodeConnection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    NodeMessage,
    NodeMessage,
    Cbor<NodeMessage, NodeMessage>,
>;

pub struct Router {
    id: NodeId,
    listener: TcpListener,
    nodes: HashMap<NodeId, NodeConnection>,
    node_addresses: HashMap<NodeId, SocketAddr>,
    pending_nodes: Vec<NodeConnection>,
    buffer: VecDeque<NodeMessage>
}

impl Router {
    pub async fn new(id: NodeId, listen_address: SocketAddr, addresses: HashMap<NodeId, SocketAddr>) -> Result<Self, Error> {
        let listener = TcpListener::bind(listen_address).await?;

        Ok(Self {
            id,
            listener,
            nodes: HashMap::new(),
            node_addresses: addresses,
            pending_nodes: vec![],
            buffer: VecDeque::new()
        })
    }
    
    pub async fn send_message(&mut self, node: NodeId, msg: NodeMessage) -> Result<(), Error> {
        if let Some(connection) = self.nodes.get_mut(&node) { 
            connection.send(msg).await?;
        } else {
            // No connection to node so if heartbeat message try to reconnect then send
            if let NodeMessage::OmniPaxosMessage(omnipaxos_core::messages::Message::BLE(_)) = msg {
                trace!("Trying to reconnect to node");
                self.add_node(node).await?;
                self.nodes.get_mut(&node).unwrap().send(msg).await?;
            }
            return Err(anyhow!("Node `{}` not connected!", node));
        }
        Ok(())
    }

    async fn add_node(&mut self, node: NodeId) -> Result<(), Error> {
        if let Some(address) = self.node_addresses.get(&node) {
            let tcp_stream = TcpStream::connect(address).await?; 
            let length_delimited = CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());
            let mut framed = Framed::new(length_delimited, Cbor::default());
            framed.send(NodeMessage::Hello(self.id)).await?;
            self.nodes.insert(node, framed);
            return Ok(());
        }
        Err(anyhow!("No known address for node {}", node))
    }
}

impl Stream for Router {
    type Item = Result<NodeMessage, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = &mut self.as_mut();


        if let Poll::Ready(val) = Pin::new(&mut self_mut.listener).poll_accept(cx) {
            match val {
                Ok((tcp_stream, socket_addr)) => {
                    debug!("New connection from {}", socket_addr);
                    let length_delimited = CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());
                    let framed = Framed::new(length_delimited, Cbor::default());
                    self_mut.pending_nodes.push(framed);
                }
                Err(err) => {
                    error!("Error checking for new requests: {:?}", err);
                    return Poll::Ready(None); // End stream
                }
            }
        }
 
        let mut new_pending = Vec::new();

        mem::swap(&mut self_mut.pending_nodes, &mut new_pending);

        for mut pending in new_pending.into_iter() {
            if let Poll::Ready(val) = Pin::new(&mut pending).poll_next(cx) {
                match val {
                    Some(Ok(NodeMessage::Hello(id))) => {
                        debug!("Node {} handshake complete",id);
                        self_mut.buffer.push_back(NodeMessage::Hello(id));
                        self_mut.nodes.insert(id, pending);
                    }
                    Some(Ok(NodeMessage::Append(kv))) => self_mut.buffer.push_back(NodeMessage::Append(kv)),
                    Some(Ok(msg)) => warn!("Received unknown message during handshake: {:?}", msg),
                    Some(Err(err)) => error!("Error checking for new requests: {:?}", err),
                    None => (),
                }
            } else {
                self_mut.pending_nodes.push(pending);
            }
        }


        let mut new_nodes = HashMap::new();

        mem::swap(&mut self_mut.nodes, &mut new_nodes);

        for (name, mut client) in new_nodes.into_iter() {
            match Pin::new(&mut client).poll_next(cx) {
                Poll::Ready(Some(Ok(val))) => {
                    trace!("Received message from node `{}`: {:?}", name, val);
                    self_mut.buffer.push_back(val);
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


        if let Some(val) = self_mut.buffer.pop_front() {
            if self_mut.buffer.len() > 0 {
                cx.waker().wake_by_ref();
            }
            return Poll::Ready(Some(Ok(val)));
        }

        return Poll::Pending;
    }
}
