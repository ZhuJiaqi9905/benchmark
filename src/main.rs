use crate::disk::file_rw::read_throughput;
use clap::Parser;
use disk::file_rw::bufread_throughput;
use rdma::read::{test_rserver, test_rclient};
use serial::serialize::test_serialize;
mod disk;
mod net;
mod rdma;
mod serial;
#[derive(Parser, Debug)]
#[clap(about, version, author)]
struct Args {
    #[clap(long)]
    bench: String,
    #[clap(long, default_value = "")]
    rdma: String,
}
fn main() {
    let args = Args::parse();
    if args.bench == "disk" {
        read_throughput("data/bigfile.log", 4096000);
        bufread_throughput("data/bigfile.log", 4096000);
    } else if args.bench == "serial" {
        test_serialize("data/bigfile.log", 1024);
    } else if args.bench == "rdma" {
        if args.rdma == "read_server" {
            test_rserver("10.0.12.24:9500", "data/bigfile.log");
        } else if args.rdma == "read_client" {
            test_rclient("10.0.12.24:9500");
        }
    }
}
