use clap::{Parser, Subcommand};
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};

#[derive(Parser)]
#[command(about = "A point cloud cli.")]
#[command(author, version, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Point cloud format conversion
    Convert(pc_io::convert::ConversionArgs),
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Convert(args)) => args.src.par_iter().for_each(|src| {
            pc_io::convert::convert(src, args.dst.as_ref(), args.overwrite, args.importance)
                .unwrap();
        }),
        None => {}
    }
}
