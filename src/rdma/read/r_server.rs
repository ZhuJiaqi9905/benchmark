use std::{
    convert::TryInto,
    fs::File,
    io::{BufReader, Read, Seek, Write},
    net::{TcpListener, TcpStream},
};

use rand::Rng;
use rdma_sys::ibv_access_flags;

use crate::rdma::verbs::{IbvContext, IbvCq, IbvMr, IbvPd, IbvQp};

// one-to-one client/server
pub(crate) struct Rserver {
    stream: TcpStream,
    mr: IbvMr,
    qp: IbvQp,
    cq: IbvCq,
    pd: IbvPd,
    context: IbvContext,
    data_buf: Box<[u8]>,
    max_cqe: i32,
}

impl Rserver {
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
        let mut data_buf =  Vec::new();
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
        let qp = IbvQp::new(&pd, &cq, &cq, 1, 10, 10, 1, 1, 10).unwrap();
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
        //send addr, len, rkey
        stream
            .write_all(&(data_buf.as_ptr() as u64).to_le_bytes())
            .unwrap();
        stream
            .write_all(&(data_buf.len() as u32).to_le_bytes())
            .unwrap();
        stream.write_all(&mr.rkey().to_le_bytes()).unwrap();
        println!(
            "my_addr: {}, my_len: {}, my_rkey: {}",
            data_buf.as_ptr() as u64,
            data_buf.len(),
            mr.rkey()
        );
        Self {
            stream,
            mr,
            qp,
            cq,
            pd,
            context,
            data_buf,
            max_cqe,
        }
    }

    pub fn wait_for_disconnect(&mut self) -> Result<bool, std::io::Error>{
        let mut buf = [0u8; 4];
        self.stream.read_exact(&mut buf)?;
        if u32::from_be_bytes(buf) == 0x10{
            Ok(true)
        }else{
            Ok(false)
        }
    }
    
}
