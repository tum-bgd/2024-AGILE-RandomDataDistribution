use clap::Parser;
use tracing_subscriber::prelude::*;

use pc_server::Config;

#[tokio::main]
async fn main() {
    // load .env
    dotenvy::dotenv().ok();

    // setup tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                // axum logs rejections from built-in extractors with the `axum::rejection`
                // target, at `TRACE` level. `axum::rejection=trace` enables showing those events
                "pc_server=debug,tower_http=debug,axum::rejection=trace".into()
            }),
        )
        .with(tracing_subscriber::fmt::layer().pretty())
        .init();

    // parse config
    let mut config = Config::parse();

    // setup listener address
    let addr = std::net::SocketAddr::new(config.host, config.port);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    let addr = listener.local_addr().unwrap();
    tracing::info!("listening on {}", addr);

    config.host = local_ip_address::local_ip().unwrap();
    config.port = addr.port();

    // register worker on coordinators
    let client = reqwest::Client::new();
    for coordinator in config.coordinators.iter() {
        client
            .post(coordinator)
            .body(config.base_url())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    // build our application
    let app = pc_server::app(config.clone());

    // run it
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal(client, config))
        .await
        .unwrap();
}

async fn shutdown_signal(client: reqwest::Client, config: Config) {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            deregister(&client, &config).await;
        },
        _ = terminate => {
            deregister(&client, &config).await;
        },
    }
}

async fn deregister(client: &reqwest::Client, config: &Config) {
    for coordinator in &config.coordinators {
        client
            .delete(coordinator)
            .body(config.base_url())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
}
