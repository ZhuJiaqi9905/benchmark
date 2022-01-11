use std::{
    convert::TryInto,
    io::{Read, Write},
    net::TcpStream,
    rc::Rc,
};

use libc::rand;
use rand::Rng;
use rdma_sys::ibv_access_flags;

use crate::rdma::verbs::{IbvContext, IbvCq, IbvMr, IbvPd, IbvQp};

pub(crate) struct Rclient {
    stream: TcpStream,
    mr: IbvMr,
    qp: IbvQp,
    cq: IbvCq,
    pd: IbvPd,
    context: IbvContext,
    recv_buf: Box<[u8]>,
    remote_addr: u64,
    remote_len: usize,
    remote_rkey: u32,
    max_cqe: i32,
}

impl Rclient {
    pub fn connect(dst: &str, buf_size: usize, max_cqe: i32) -> Self {
        let mut stream = TcpStream::connect(dst).unwrap();
        let context = IbvContext::new(Some("mlx5_1")).unwrap();
        let pd = IbvPd::new(&context).unwrap();

        let access_flag = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE;
        let mut recv_buf = vec![0; buf_size].into_boxed_slice();
        let mr = IbvMr::new(&pd, &mut recv_buf, access_flag).unwrap();
        let cq = IbvCq::new(&context, max_cqe).unwrap();
        let qp = IbvQp::new(&pd, &cq, &cq, 1, 10, 10, 1, 1, 10).unwrap();
        qp.modify_reset2init(1).unwrap();
        let my_qpn = qp.get_qpn();
        let my_psn = rand::thread_rng().gen::<u32>();
        let my_lid = context.get_lid(1).unwrap();
        stream.write_all(&my_qpn.to_le_bytes());
        stream.write_all(&my_psn.to_le_bytes());
        stream.write_all(&my_lid.to_le_bytes());
        stream.flush().unwrap();
        println!("my_qpn: {}, my_psn: {}, my_lid: {}", my_qpn, my_psn, my_lid);
        let mut buf = [0u8; 1024];
        let mut data_size = 10;
        let mut meta_data: Vec<u8> = vec![];
        while data_size > 0 {
            let len = stream.read(&mut buf).unwrap();
            meta_data.extend_from_slice(&buf[0..len]);
            data_size -= len;
        }
        let remote_qpn = u32::from_le_bytes(meta_data[0..4].try_into().unwrap());
        let remote_psn = u32::from_le_bytes(meta_data[4..8].try_into().unwrap());
        let remote_lid = u16::from_le_bytes(meta_data[8..10].try_into().unwrap());
        println!(
            "remote_qpn: {}, remote_psn: {}, remote_lid: {}",
            remote_qpn, remote_psn, remote_lid
        );
        qp.modify_init2rtr(0, 1, remote_qpn, remote_psn, remote_lid)
            .unwrap();
        qp.modify_rtr2rts(my_psn).unwrap();
        // get remote addr, remote len, rkey
        data_size = 16;
        meta_data.clear();
        while data_size > 0 {
            let len = stream.read(&mut buf).unwrap();
            meta_data.extend_from_slice(&buf[0..len]);
            data_size -= len;
        }
        let remote_addr = u64::from_le_bytes(meta_data[0..8].try_into().unwrap());
        let remote_len = u32::from_le_bytes(meta_data[8..12].try_into().unwrap()) as usize;
        let remote_rkey = u32::from_le_bytes(meta_data[12..16].try_into().unwrap());
        println!(
            "remote_addr: {}, remote_len: {}, remote_rkey: {}",
            remote_addr, remote_len, remote_rkey
        );
        Self {
            stream,
            mr,
            qp,
            cq,
            pd,
            context,
            recv_buf,
            remote_addr,
            remote_len,
            remote_rkey,
            max_cqe,
        }
    }
    pub fn disconnect(&mut self) {
        self.stream.write_all(&0x10_i32.to_le_bytes()).unwrap();
        println!("disconnect");
    }
}
