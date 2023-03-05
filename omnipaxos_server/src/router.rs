use std::net::SocketAddr;
use std::{collections::HashMap};
use futures::{SinkExt, Stream};
use serde_json::{json, Value};
use tokio::net::{TcpStream, TcpListener};
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::{formats::Json, Framed};
use tokio_util::codec::{Framed as CodecFramed, LengthDelimitedCodec, FramedWrite, FramedParts};
use anyhow::{anyhow, Error};
use std::pin::Pin;
use std::task::{Context, Poll};

use omnipaxos_core::{util::NodeId, messages::Message};


use std::mem;
use log::*;

use crate::kv::{KVSnapshot, KeyValue};

type FramedServer = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    Value,
    Value,
    Json<Value, Value>,
>;

struct Router {
    clients: HashMap<NodeId, FramedServer>,
    listener: TcpListener,
    pending_clients: Vec<FramedServer>
}

impl Router {
    pub async fn new(addr: &str) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await?;

        Ok(Self {
            clients: HashMap::new(),
            listener,
            pending_clients: vec![]
        })
    }
    
    pub async fn send_message(&mut self, node: NodeId, msg: Message<KeyValue, KVSnapshot>) -> Result<(), Error> {
        if let Some(connection) = self.clients.get_mut(&node) { 
            connection.send(json!(msg)).await?;
        } else {
            return Err(anyhow!("Node `{}` not connected!", node));
        }

        Ok(())
    }
}

impl Stream for Router {
    type Item = Result<Message<KeyValue, KVSnapshot>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = &mut self.as_mut();

        if let Poll::Ready(val) = Pin::new(&mut self_mut.listener).poll_accept(cx) {
            match val {
                Ok((tcp_stream, socket_addr)) => {
                    // Delimit frames using a length header
                    let length_delimited =
                        CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());
                    // Serialize frames with JSON
                    let framed = Framed::new(length_delimited, Json::default()); 
                    self_mut.pending_clients.push(framed);
                }
                Err(err) => {
                    error!("Error checking for new requests:{:?}", err);
                    return Poll::Ready(None);
                }
            }
        }


        let mut new_pending = Vec::new();

        mem::swap(&mut self_mut.pending_clients, &mut new_pending);

        for mut pending in new_pending.into_iter() {
            if let Poll::Ready(val) = Pin::new(&mut pending).poll_next(cx) {
                match val {
                    Some(Ok(ServerMessage::Hello(name))) => {
                        debug!("New Client connection from `{}`", name);
                        self_mut.buffer.push(ServerMessage::Hello(name.clone()));
                        self_mut.clients.insert(name, pending);
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
                self_mut.pending_clients.push(pending);
            }
        }


        let mut new_clients = HashMap::new();

        mem::swap(&mut self_mut.clients, &mut new_clients);

        for (name, mut client) in new_clients.into_iter() {
            match Pin::new(&mut client).poll_next(cx) {
                Poll::Ready(Some(Ok(val))) => {
                    trace!("Received message from `{}`: {:?}", name, val);
                    self_mut.buffer.push(val);
                    self_mut.clients.insert(name, client);
                }
                Poll::Ready(None) => {
                    //Finished
                    debug!("Client `{}` disconnecting", name);
                }
                Poll::Ready(Some(Err(err))) => {
                    //Error
                    error!("Error from `{}`: {} Removing connection.", name, err);
                }
                Poll::Pending => {
                    self_mut.clients.insert(name, client);
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
