use benchmark::connection::{client_disconnect, connect, MemInfo};
use clap::Parser;
use rdma_rs::{
    ffi::{ibv_access_flags, ibv_send_wr, ibv_sge, ibv_wc, ibv_wr_opcode},
    ibv::{IbvContext, IbvCq, IbvMr, IbvPd, IbvQp},
};
use std::{net::TcpStream, time::SystemTime};

#[derive(Parser, Debug)]
#[clap(about, version, author)]
struct Args {
    #[clap(long, short = 'p', default_value_t = 1)]
    ib_port: u8,
    #[clap(long, short, default_value = "mlx5_1")]
    dev: String,
    #[clap(long, short = 's', default_value_t = 1024)]
    buf_size: usize,
    #[clap(long, short, default_value_t = 512)]
    message_size: usize,
    #[clap(long, short, default_value_t = 1)]
    qp_threads: usize,
    #[clap(long, short, default_value_t = 4096)]
    tx_depth: i32,
    #[clap(long, short = 'a', default_value = "127.0.0.1:9900")]
    addr: String,
}

fn main() {
    println!("hello world from read_client");
    // Create RDMA resources.
    let args = Args::parse();
    let mut buf = vec![0_u8; args.buf_size].into_boxed_slice();
    let context = IbvContext::new(Some(&args.dev)).unwrap();
    let pd = IbvPd::new(&context).unwrap();
    let access_flag = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE;
    let mr = IbvMr::new(&pd, &mut buf, access_flag).unwrap();
    let cq = IbvCq::new::<()>(&context, args.tx_depth + 10, None, None, 0).unwrap();
    let qp = IbvQp::new(&pd, &cq, &cq, 1, args.tx_depth as u32, 10, 1, 1, 0).unwrap();
    // Connection.
    let mut stream = TcpStream::connect(&args.addr).unwrap();
    let my_lid = context.query_port(args.ib_port).unwrap().lid();
    let remote_mem = connect(&mut stream, &qp, my_lid, &buf, &mr);
    let my_mem = MemInfo::new(buf.as_ptr() as u64, buf.len(), mr.rkey());
    //RDMA read.
    rdma_read(&qp, &cq, &mr, my_mem, remote_mem, &args);
    // Close Connection.
    client_disconnect(stream);
}

fn rdma_read(
    qp: &IbvQp,
    cq: &IbvCq,
    mr: &IbvMr,
    my_mem: MemInfo,
    remote_mem: MemInfo,
    args: &Args,
) {
    if my_mem.len < remote_mem.len {
        println!("Memory len error. Remote len < my len");
    }
    let msg_len = args.message_size;
    let tx_depth = args.tx_depth;
    let num = my_mem.len / msg_len;
    let mut inflight_wr = 0;
    let qp = qp.as_mut_ptr();
    let cq = cq.as_mut_ptr();
    let mut offset = 0;
    let lkey = mr.lkey();
    let mut cqe_arr = vec![unsafe { std::mem::zeroed::<ibv_wc>() }; 1].into_boxed_slice();
    let ibv_post_send = unsafe { (*(*qp).context).ops.post_send.unwrap() };
    let ibv_poll_cq = unsafe { (*(*cq).context).ops.poll_cq.unwrap() };
    let start = SystemTime::now();
    for _ in 0..num {
        // RDMA post send.
        let mut sge = ibv_sge {
            addr: my_mem.addr + offset,
            length: msg_len as u32,
            lkey,
        };
        let mut wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
        wr.next = std::ptr::null_mut::<ibv_send_wr>() as *mut _;
        wr.sg_list = &mut sge as *mut _;
        wr.num_sge = 1;
        wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_READ;
        wr.wr.rdma.remote_addr = remote_mem.addr + offset;
        wr.wr.rdma.rkey = remote_mem.rkey;
        wr.send_flags = 0;
        let ret = unsafe { ibv_post_send(qp, &wr as *const _ as *mut _, std::ptr::null_mut()) };
        if ret != 0 {
            println!("ibv_post_send error");
        }

        offset += msg_len as u64;
        inflight_wr += 1;
        if inflight_wr == tx_depth {
            loop {
                let res = unsafe { ibv_poll_cq(cq, 1, cqe_arr.as_mut_ptr()) };
                if res > 0 {
                    inflight_wr -= res;
                    break;
                } else if res < 0 {
                    println!("ibv_poll_cq error");
                }
            }
        }
    }
    while inflight_wr > 0 {
        let res = unsafe { ibv_poll_cq(cq, 1, cqe_arr.as_mut_ptr()) };
        if res > 0 {
            inflight_wr -= res;
        } else if res < 0 {
            println!("ibv_poll_cq error");
        }
    }
    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap().as_micros();
    println!("duration {}us", duration);
    let duration = (duration as f64) / 1e6;
    let total_size = (8 * num * msg_len) as f64 / (1024f64 * 1024f64 * 1024f64);
    let throughput = total_size / duration;
    println!("rdma read throughput: {:.3}Gbps", throughput);
}
