use clap::Parser;
use serde::Serialize;

/// Service configuration
#[derive(Parser, Debug, Clone, Serialize)]
#[command(name = "Service")]
#[command(author, version, about, long_about = None)]
pub struct Config {
    /// Service host
    #[arg(long, env = "HOST", value_parser, default_value = "0.0.0.0")]
    pub host: std::net::IpAddr,

    /// Service port
    #[arg(long, env = "PORT", default_value = "0")]
    pub port: u16,

    /// Coordinator urls
    #[arg(long, env = "COORDINATORS")]
    pub coordinators: Vec<String>,
}

impl Config {
    pub fn base_url(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }
}
