use std::net::SocketAddr;
use std::{collections::HashMap};
use futures::{SinkExt, Stream};
use serde_json::{json, Value};
use tokio::net::{TcpStream, TcpListener};
use tokio_serde::{formats::{Json, Cbor}, Framed};
use tokio_util::codec::{Framed as CodecFramed, LengthDelimitedCodec};
use anyhow::{anyhow, Error};
use std::pin::Pin;
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
    pending_nodes: Vec<NodeConnection>,
    buffer: Vec<NodeMessage>
}

impl Router {
    pub async fn new(addr: &str) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await?;

        Ok(Self {
            listener,
            nodes: HashMap::new(),
            pending_nodes: vec![],
            buffer: vec![]
        })
    }
    
    pub async fn send_message(&mut self, node: NodeId, msg: NodeMessage) -> Result<(), Error> {
        if let Some(connection) = self.nodes.get_mut(&node) { 
            connection.send(msg).await?;
        } else {
            return Err(anyhow!("Node `{}` not connected!", node));
        }
        Ok(())
    }

    pub async fn add_connection(&mut self, node: NodeId, address: &str) -> Result<(), Error> {
        let tcp_stream = TcpStream::connect(address).await?; 
        let length_delimited = CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());
        let framed = Framed::new(length_delimited, Cbor::default());
        self.pending_nodes.push(framed);
        Ok(())
    }
}

impl Stream for Router {
    type Item = Result<NodeMessage, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = &mut self.as_mut();


        if let Poll::Ready(val) = Pin::new(&mut self_mut.listener).poll_accept(cx) {
            match val {
                Ok((tcp_stream, socket_addr)) => {
                    let length_delimited = CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());
                    let framed = Framed::new(length_delimited, Cbor::default());
                    self_mut.pending_nodes.push(framed);
                }
                Err(err) => {
                    error!("Error checking for new requests:{:?}", err);
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
