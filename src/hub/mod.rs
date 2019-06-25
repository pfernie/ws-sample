use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time;

use futures::future::{self, Future};
use futures::sink::Sink;
use futures::stream::{self, Stream};
use futures::sync::{mpsc, oneshot};
use log::{debug, error, info};
use tokio::timer::Interval;

struct Message {
    from: Option<u16>,
    text: String,
}

impl Message {
    fn from<S: Into<String>>(id: u16, text: S) -> Message {
        Message {
            from: Some(id),
            text: text.into(),
        }
    }

    fn broadcast<S: Into<String>>(text: S) -> Message {
        Message {
            from: None,
            text: text.into(),
        }
    }
}

enum Command {
    Message(Message),
    Add(mpsc::Sender<String>, oneshot::Sender<u16>),
    Remove(u16),
}

#[derive(Clone)]
pub struct ClientFactory(mpsc::Sender<Command>);

impl ClientFactory {
    pub fn add_client(
        &self,
    ) -> impl Future<Item = (Client, impl stream::Stream<Item = String, Error = ()>), Error = ()>
    {
        let (broadcast_tx, broadcasts) = mpsc::channel(1_024);
        let (id_tx, id_rx) = oneshot::channel();
        let hub_ctrl = self.0.clone();
        self.0
            .clone()
            .send(Command::Add(broadcast_tx, id_tx))
            .map(|_| {
                debug!(
                    "ClientFactory::add_client: {:?}",
                    std::thread::current().id()
                )
            })
            .map_err(|e| error!("error asking Hub to Add: {:?}", e))
            .and_then(|_| {
                id_rx.map_err(|_| ()).map(move |id| {
                    let client = Client {
                        id,
                        hub: hub_ctrl.clone(),
                    };

                    let broadcasts =
                        stream::once::<_, ()>(Ok(format!("welcome, you are client {}", id)))
                            .chain(broadcasts);
                    (client, broadcasts)
                })
            })
    }
}

#[must_use]
pub struct Client {
    id: u16,
    hub: mpsc::Sender<Command>,
}

impl Client {
    pub fn id(&self) -> u16 {
        self.id
    }

    pub fn consume_stream<S>(&self, stream: S) -> impl Future<Item = (), Error = ()>
    where
        S: 'static + stream::Stream<Item = Option<String>, Error = ()> + std::marker::Send,
    {
        let hub_ctrl = self.hub.clone();
        let id = self.id();
        stream
            .map(move |s| {
                if let Some(s) = s {
                    Command::Message(Message::from(id, s))
                } else {
                    Command::Remove(id)
                }
            })
            .for_each(move |cmd| {
                hub_ctrl
                    .clone()
                    .send(cmd)
                    .map(|_| ())
                    .map_err(|e| error!("error delivering incoming msg to Hub: {:?}", e))
            })
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        debug!(
            "Client::drop() called on thread {:?}",
            std::thread::current().id()
        );
        self.hub
            .clone()
            .send(Command::Remove(self.id))
            .wait()
            .unwrap();
    }
}

pub struct Hub {
    next_id: AtomicU16,
    clients: HashMap<u16, mpsc::Sender<String>>,
}

impl Hub {
    pub fn build() -> (
        ClientFactory,
        impl stream::Stream<Item = impl Future<Item = (), Error = ()>, Error = ()>,
    ) {
        use futures::Poll;
        #[derive(Debug)]
        enum CommandFuture<A, R, M> {
            Add(A),
            Remove(R),
            Message(M),
        }

        impl<A, R, M> Future for CommandFuture<A, R, M>
        where
            A: Future,
            R: Future<Item = A::Item, Error = A::Error>,
            M: Future<Item = A::Item, Error = A::Error>,
        {
            type Item = A::Item;
            type Error = A::Error;

            fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
                match *self {
                    CommandFuture::Add(ref mut a) => a.poll(),
                    CommandFuture::Remove(ref mut r) => r.poll(),
                    CommandFuture::Message(ref mut m) => m.poll(),
                }
            }
        }

        let (tx, rx) = mpsc::channel(1_024);
        let tick_dur = time::Duration::from_secs(5);
        let ticks = Interval::new_interval(tick_dur)
            .inspect(|t| info!("local tick: {:?}", t))
            .map(|t| Command::Message(Message::broadcast(format!("broadcast tick: {:?}", t))))
            .map_err(|e| error!("error generating interval tick: {:?}", e));

        let mut hub = Hub {
            next_id: AtomicU16::new(0),
            clients: HashMap::new(),
        };

        let hub_loop = rx.select(ticks).filter_map(move |c| match c {
            Command::Add(s, id_tx) => {
                info!("Hub received Add");
                let id = hub.add(s.clone());
                Some(CommandFuture::Add(id.and_then(|id| {
                    id_tx
                        .send(id)
                        .map_err(|e| error!("failed to respond with id: {:?}", e))
                        .map(|_| ())
                })))
            }
            Command::Remove(rm) => {
                info!("Hub received Remove for {}", rm);
                hub.remove(rm).map(CommandFuture::Remove)
            }
            Command::Message(msg) => {
                if let Some(ref from) = msg.from {
                    info!("Hub received Message from {}: '{}'", from, msg.text);
                } else {
                    info!("Hub received Broadcast message: '{}'", msg.text);
                }
                Some(CommandFuture::Message(hub.broadcast(msg)))
            }
        });
        (ClientFactory(tx), hub_loop)
    }

    fn broadcast(&self, msg: Message) -> impl Future<Item = (), Error = ()> {
        let Message { from, text } = msg;
        let clients = self
            .clients
            .iter()
            .filter_map(move |(id, c)| {
                if !from.map(|f| f == *id).unwrap_or(false) {
                    Some((*c).clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        stream::iter_ok::<_, ()>(clients.into_iter()).for_each(move |c| {
            c.send(text.clone()).wait().unwrap();
            Ok(())
        })
    }

    fn add(&mut self, s: mpsc::Sender<String>) -> impl Future<Item = u16, Error = ()> {
        let id = self.next_id.fetch_add(1u16, Ordering::SeqCst);
        self.clients.insert(id, s);
        self.broadcast(Message::broadcast(format!("client {} joined", id)))
            .then(move |_| future::ok(id))
    }

    fn remove(&mut self, rm: u16) -> Option<impl Future<Item = (), Error = ()>> {
        self.clients.remove(&rm).map(move |_| {
            self.broadcast(Message::broadcast(format!(
                "client {} left, we shall miss them",
                rm
            )))
        })
    }
}
