use std::{
    arch::x86_64::_CMP_EQ_UQ,
    convert::TryInto,
    io::{Read, Write},
    net::TcpStream,
    rc::Rc, fs,
};

use libc::rand;
use rand::Rng;
use rdma_sys::{ibv_access_flags, ibv_send_flags, ibv_wc, ibv_wc_opcode, ibv_wc_status};

use crate::rdma::verbs::{post_read, IbvContext, IbvCq, IbvMr, IbvPd, IbvQp};

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
        let qp = IbvQp::new(&pd, &cq, &cq, 0, max_cqe as u32, max_cqe as u32, 1, 1, 10).unwrap();
        qp.modify_reset2init(1).unwrap();
        let my_qpn = qp.get_qpn();
        let my_psn = rand::thread_rng().gen::<u32>();
        let my_lid = context.get_lid(1).unwrap();
        stream.write_all(&my_qpn.to_le_bytes()).unwrap();
        stream.write_all(&my_psn.to_le_bytes()).unwrap();
        stream.write_all(&my_lid.to_le_bytes()).unwrap();
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
    pub fn read_data(&mut self, bench_size: usize) {
        let mut s = 0;
        let mut wr_id = 0;
        let mut cqe = 1;
        let mut cqe_arr = unsafe { [std::mem::zeroed::<ibv_wc>(); 1] };
        // read data by rdma read
        for _i in 0..(self.remote_len / bench_size) {
            if cqe < self.max_cqe {
                post_read(
                    &self.recv_buf[s..(s + bench_size)],
                    self.remote_addr + s as u64,
                    self.remote_rkey,
                    &self.qp,
                    &self.mr,
                    wr_id,
                    0,
                );
            } else {
                post_read(
                    &self.recv_buf[s..(s + bench_size)],
                    self.remote_addr + s as u64,
                    self.remote_rkey,
                    &self.qp,
                    &self.mr,
                    wr_id,
                    ibv_send_flags::IBV_SEND_SIGNALED.0,
                );
                loop {
                    let res = self.cq.poll(&mut cqe_arr);
                    if res.len() > 0 {
                        if res[0].status != ibv_wc_status::IBV_WC_SUCCESS
                            || res[0].opcode != ibv_wc_opcode::IBV_WC_RDMA_READ
                        {
                            panic!("read completion error, status is {}", res[0].status);
                        }
                        break;
                    }
                }
            }
            cqe += 1;
            wr_id += 1;
            s += bench_size;
        }
        if s < self.remote_len {
            post_read(
                &self.recv_buf[s..self.remote_len],
                self.remote_addr + s as u64,
                self.remote_rkey,
                &self.qp,
                &self.mr,
                wr_id,
                ibv_send_flags::IBV_SEND_SIGNALED.0,
            );
            loop {
                let res = self.cq.poll(&mut cqe_arr);
                if res.len() > 0 {
                    if res[0].status != ibv_wc_status::IBV_WC_SUCCESS
                        || res[0].opcode != ibv_wc_opcode::IBV_WC_RDMA_READ
                    {
                        panic!("read completion error, status is {}", res[0].status);
                    }
                    break;
                }
            }
        }
        //write data to the out file
        let mut out_file = fs::File::open("log/smalldata.log").unwrap();
        out_file.write_all(&self.recv_buf[0..self.remote_len]).unwrap();
    }
}