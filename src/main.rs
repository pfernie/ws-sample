#![deny(warnings)]
use futures::future::lazy;
use futures::sink::Sink;
use log::{debug, error, info};
use pretty_env_logger;
use tokio;
use warp::filters::ws::Message;
use warp::{Filter, Future, Stream};

mod hub;

fn main() -> Result<(), String> {
    debug!("main thread: {:?}", std::thread::current().id());
    pretty_env_logger::init();

    tokio::run(lazy(|| {
        debug!("tokio::run thread: {:?}", std::thread::current().id());

        let (client_factory, hub_stream) = hub::Hub::build();
        tokio::spawn(hub_stream.for_each(|hub_fut| {
            tokio::spawn(hub_fut);
            futures::future::ok(())
        }));
        info!("hub spawned");

        let routes = warp::path("chat")
            .and(warp::ws2()) // adds handling for websocket handshake
            .map(move |ws: warp::ws::Ws2| {
                let client_factory = client_factory.clone();
                ws.on_upgrade(move |ws| {
                    client_factory
                        .add_client()
                        .and_then(|(client, broadcasts)| {
                            let (ws_tx, ws_rx) = ws.split();
                            let ws_rx = ws_rx
                                .filter_map(|msg| {
                                    if msg.is_close() {
                                        Some(None)
                                    } else if msg.is_text() {
                                        Some(msg.to_str().map(String::from).ok())
                                    } else {
                                        None
                                    }
                                })
                                .map_err(|e| error!("error reading socket: {}", e));
                            tokio::spawn(client.consume_stream(ws_rx));

                            let ws_tx = ws_tx.sink_map_err(move |e| {
                                info!("client connection {} errored with: {:?}", client.id(), e);
                            });
                            broadcasts.map(Message::text).forward(ws_tx).map(|_| ()) // discard stream/sink
                        })
                })
            });
        let warp_server = warp::serve(routes).bind(([127, 0, 0, 1], 3030));
        tokio::spawn(warp_server);
        info!("warp server started");
        Ok(())
    }));
    Ok(())
}
