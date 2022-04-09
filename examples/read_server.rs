use benchmark::connection::{connect, server_disconnect, MemInfo};
use clap::Parser;
use rdma_rs::{
    ffi::ibv_access_flags,
    ibv::{IbvContext, IbvCq, IbvMr, IbvPd, IbvQp},
};
use std::{
    net::{TcpListener, TcpStream},
    vec::Vec,
};
#[derive(Parser, Debug)]
#[clap(about, version, author)]
struct Args {
    #[clap(long, short = 'p', default_value_t = 1)]
    ib_port: u8,
    #[clap(long, short, default_value = "mlx5_1")]
    dev: String,
    #[clap(long, short = 's', default_value_t = 1024)]
    buf_size: usize,
    #[clap(long, short, default_value_t = 1)]
    qp_threads: usize,
    #[clap(long, short, default_value_t = 4096)]
    tx_depth: i32,
    #[clap(long, short = 'a', default_value = "127.0.0.1:9900")]
    addr: String,
}
fn listen(listener: &TcpListener) -> TcpStream {
    loop {
        for s in listener.incoming() {
            let stream = s.unwrap();
            return stream;
        }
    }
}
fn main() {
    // Create RDMA resources.
    let args = Args::parse();
    let mut listener = TcpListener::bind(&args.addr).unwrap();
    println!("listen on {}", args.addr);
    let mut stream = listen(&listener);
    let mut buf = vec![1_u8; args.buf_size].into_boxed_slice();
    let context = IbvContext::new(Some(&args.dev)).unwrap();
    let pd = IbvPd::new(&context).unwrap();
    let access_flag = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE;
    let mr = IbvMr::new(&pd, &mut buf, access_flag).unwrap();
    let cq = IbvCq::new::<()>(&context, args.tx_depth + 10, None, None, 0).unwrap();
    let qp = IbvQp::new(&pd, &cq, &cq, 1, args.tx_depth as u32, 10, 1, 1, 0).unwrap();
    // Connection.
    let my_lid = context.query_port(args.ib_port).unwrap().lid();
    let remote_mem = connect(&mut stream, &qp, my_lid, &buf, &mr);

    // Wait for disonnection.
    server_disconnect(stream);
}
