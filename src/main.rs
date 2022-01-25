#![allow(dead_code, unused_imports)]
use crate::disk::file_rw::read_throughput;
use clap::Parser;
use disk::file_rw::{bufread_throughput, bufwrite_throughput};
use rdma::{read::{test_rclient, test_rserver}, write::{test_wserver, test_wclient}};
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
    #[clap(long, default_value = "")]
    disk: String,
}

fn main() {
    let args = Args::parse();
    if args.bench == "disk" {
        if args.disk == "read" {
            read_throughput("data/bigfile.log", 4096000);
            bufread_throughput("data/bigfile.log", 4096000);
        } else if args.disk == "write" {
            bufwrite_throughput("data/bigfile.log", "log/bigfile.log", 1024000);
        }
    } else if args.bench == "serial" {
        test_serialize("data/bigfile.log", 1024);
    } else if args.bench == "rdma" {
        if args.rdma == "read_server" {
            test_rserver("10.0.12.24:9500", "data/bigfile.log");
        } else if args.rdma == "read_client" {
            test_rclient("10.0.12.24:9500");
        }else if args.rdma == "write_server"{
            test_wserver("10.0.12.24:9500", "data/bigfile.log");
        }else if args.rdma == "write_client"{
            test_wclient("10.0.12.24:9500");
        }

    }
}
