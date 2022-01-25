use std::{
    arch::x86_64::_CMP_EQ_UQ,
    convert::TryInto,
    fs,
    io::{Read, Write},
    net::TcpStream,
    rc::Rc,
    time::SystemTime,
};

use libc::rand;
use rand::Rng;
use rdma_sys::{ibv_access_flags, ibv_send_flags, ibv_wc, ibv_wc_opcode, ibv_wc_status};

use crate::rdma::verbs::{post_write, IbvContext, IbvCq, IbvMr, IbvPd, IbvQp};

pub(crate) struct Wclient {
    stream: TcpStream,
    mr: IbvMr,
    qp: IbvQp,
    cq: IbvCq,
    pd: IbvPd,
    context: IbvContext,
    recv_buf: Box<[u8]>,
    max_cqe: i32,
}

impl Wclient {
    pub fn connect(dst: &str, max_cqe: i32) -> Self {
        let mut stream = TcpStream::connect(dst).unwrap();
        let context = IbvContext::new(Some("mlx5_1")).unwrap();
        let pd = IbvPd::new(&context).unwrap();

        let access_flag = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE;
        
        let cq = IbvCq::new(&context, max_cqe).unwrap();
        let qp = IbvQp::new(&pd, &cq, &cq, 1, max_cqe as u32, max_cqe as u32, 1, 1, 10).unwrap();
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
        // read buf_size
        stream.read_exact(&mut buf[0..4]).unwrap();
        let buf_size = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let mut recv_buf = vec![0; buf_size].into_boxed_slice();
        let mr = IbvMr::new(&pd, &mut recv_buf, access_flag).unwrap();
        println!("remote len: {}", buf_size);
        //send addr, rkey
        stream
            .write_all(&(recv_buf.as_ptr() as u64).to_le_bytes())
            .unwrap();
        stream.write_all(&mr.rkey().to_le_bytes()).unwrap();
        println!(
            "my_addr: {}, my_rkey: {}",
            recv_buf.as_ptr() as u64,
            mr.rkey()
        );

        Self {
            stream,
            mr,
            qp,
            cq,
            pd,
            context,
            recv_buf,
            max_cqe,
        }
    }
    pub fn wait_for_disconnect(&mut self) {
        let mut buf = [0u8; 4];
        self.stream.read_exact(&mut buf).unwrap();
        println!("receive disconnection signal");
        //write data to the out file
        let mut out_file = fs::File::create("log/rdma_write.log").unwrap();
        out_file.write_all(&self.recv_buf[..]).unwrap();
        println!("write data finish");
    }
}
