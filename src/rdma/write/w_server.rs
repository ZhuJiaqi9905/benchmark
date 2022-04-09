use std::{
    convert::TryInto,
    fs::File,
    io::{BufReader, Read, Seek, Write},
    net::{TcpListener, TcpStream},
    time::SystemTime,
};

use rand::Rng;
use rdma_sys::{ibv_access_flags, ibv_send_flags, ibv_wc, ibv_wc_opcode, ibv_wc_status};

use crate::rdma::verbs::{post_write, post_write_raw, IbvContext, IbvCq, IbvMr, IbvPd, IbvQp};

// one-to-one client/server
pub(crate) struct Wserver {
    stream: TcpStream,
    mr: IbvMr,
    qp: IbvQp,
    cq: IbvCq,
    pd: IbvPd,
    context: IbvContext,
    data_buf: Box<[u8]>,
    max_cqe: i32,
    remote_addr: u64,
    remote_rkey: u32,
}

impl Wserver {
    pub fn listen_one(listen_addr: &str) -> TcpStream {
        let listener = TcpListener::bind(listen_addr).unwrap();
        println!("listen on {}", listen_addr);
        loop {
            for s in listener.incoming() {
                let stream = s.unwrap();
                return stream;
            }
        }
    }
    pub fn new(mut stream: TcpStream, in_path: &str, max_cqe: i32) -> Self {
        // read all the data to data buf
        let mut file = BufReader::new(File::open(in_path).expect("Unable to open input file"));
        let mut data_buf = Vec::new();
        file.read_to_end(&mut data_buf).unwrap();
        let mut data_buf = data_buf.into_boxed_slice();
        // init rdma connection
        let access_flag = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE;
        let context = IbvContext::new(Some("mlx5_1")).unwrap();
        let pd = IbvPd::new(&context).unwrap();

        let mr = IbvMr::new(&pd, &mut data_buf, access_flag).unwrap();
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
        println!("init2rtr");
        qp.modify_rtr2rts(my_psn).unwrap();
        // send data_len
        stream
            .write_all(&(data_buf.len() as u32).to_le_bytes())
            .unwrap();
        // get remote addr, rkey
        data_size = 12;
        meta_data.clear();
        while data_size > 0 {
            let len = stream.read(&mut buf).unwrap();
            meta_data.extend_from_slice(&buf[0..len]);
            data_size -= len;
        }
        let remote_addr = u64::from_le_bytes(meta_data[0..8].try_into().unwrap());
        let remote_rkey = u32::from_le_bytes(meta_data[8..12].try_into().unwrap());
        println!("remote_addr: {}, remote_rkey: {}", remote_addr, remote_rkey);
        Self {
            stream,
            mr,
            qp,
            cq,
            pd,
            context,
            data_buf,
            max_cqe,
            remote_addr,
            remote_rkey,
        }
    }

    pub fn write_data(&mut self, batch_size: usize) {
        let mut s = 0;
        let mut wr_id = 0;
        let mut cqe = 0;
        let mut cqe_arr = Vec::with_capacity(1024);
        for i in 0..1024 {
            let c = unsafe { std::mem::zeroed::<ibv_wc>() };
            cqe_arr.push(c);
        }
        // write data by rdma write
        let start = SystemTime::now();
        println!("start write data");
        for _i in 0..(self.data_buf.len() / batch_size) {
            post_write(
                &self.data_buf[s..(s + batch_size)],
                self.remote_addr + s as u64,
                self.remote_rkey,
                &self.qp,
                &self.mr,
                wr_id,
                ibv_send_flags::IBV_SEND_SIGNALED.0,
            );
            cqe += 1;
            wr_id += 1;
            s += batch_size;
            // println!("post write");
            if cqe == self.max_cqe {
                loop {
                    let res = self.cq.poll(&mut cqe_arr);
                    if res.len() > 0 {
                        cqe -= res.len() as i32;
                        // println!("poll {} cqes", res.len());
                        break;
                    }
                }
            }
        }
        if s < self.data_buf.len() {
            post_write(
                &self.data_buf[s..self.data_buf.len()],
                self.remote_addr + s as u64,
                self.remote_rkey,
                &self.qp,
                &self.mr,
                wr_id,
                ibv_send_flags::IBV_SEND_SIGNALED.0,
            );
            cqe += 1;
        }
        // println!("cqe num {}", cqe);
        while cqe > 0 {
            let res = self.cq.poll(&mut cqe_arr);
            // println!("poll num {}", res.len());
            if res.len() > 0 {
                cqe -= res.len() as i32;
            }
        }
        let end = SystemTime::now();
        let duration = (end.duration_since(start).unwrap().as_micros() as f64) / 1e6;
        let total_size = self.data_buf.len() as f64 / (1024f64 * 1024f64);
        let throughput = total_size / duration;
        println!("rdma write throughput: {:.3}MB/s", throughput);
    }

    pub fn disconnect(&mut self) {
        self.stream.write_all(&0x10_i32.to_le_bytes()).unwrap();
        println!("disconnect");
    }
}
