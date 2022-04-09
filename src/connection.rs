use std::{
    convert::TryInto,
    io::{Read, Write},
    net::TcpStream,
};

use rand::Rng;
use rdma_rs::ibv::{IbvMr, IbvQp};

pub fn connect(stream: &mut TcpStream, qp: &IbvQp, my_lid: u16, buf: &[u8], mr: &IbvMr) -> MemInfo {
    // Send qpn, psn, lid.
    qp.modify_reset2init(1).unwrap();
    let my_qpn = qp.qpn();
    let my_psn = rand::thread_rng().gen::<u32>();
    stream.write_all(&my_qpn.to_le_bytes()).unwrap();
    stream.write_all(&my_psn.to_le_bytes()).unwrap();
    stream.write_all(&my_lid.to_le_bytes()).unwrap();
    stream.flush().unwrap();
    println!("my_qpn: {}, my_psn: {}, my_lid: {}", my_qpn, my_psn, my_lid);
    //Send addr, len, rkey.
    stream
        .write_all(&(buf.as_ptr() as u64).to_le_bytes())
        .unwrap();
    stream.write_all(&(buf.len() as u32).to_le_bytes()).unwrap();
    stream.write_all(&mr.rkey().to_le_bytes()).unwrap();
    let mut tcp_buf = [0u8; 1024];
    let mut data_size = 26;
    let mut meta_data: Vec<u8> = vec![];
    while data_size > 0 {
        let len = stream.read(&mut tcp_buf).unwrap();
        meta_data.extend_from_slice(&tcp_buf[0..len]);
        data_size -= len;
    }
    // Get remote qpn, psn, lid.
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
    // Get remote addr, len, rkey.
    
    let remote_addr = u64::from_le_bytes(meta_data[10..18].try_into().unwrap());
    let remote_len = u32::from_le_bytes(meta_data[18..22].try_into().unwrap()) as usize;
    let remote_rkey = u32::from_le_bytes(meta_data[22..26].try_into().unwrap());
    println!(
        "remote_addr: {}, remote_len: {}, remote_rkey: {}",
        remote_addr, remote_len, remote_rkey
    );
    MemInfo::new(remote_addr, remote_len, remote_rkey)
}

pub struct MemInfo {
    pub addr: u64,
    pub len: usize,
    pub rkey: u32,
}
impl MemInfo {
    pub fn new(addr: u64, len: usize, rkey: u32) -> Self {
        Self { addr, len, rkey }
    }
}

pub fn client_disconnect(mut stream: TcpStream) {
    stream.write_all(&42_i32.to_le_bytes()).unwrap();
    println!("client disconnect");
}
pub fn server_disconnect(mut stream: TcpStream) {
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).unwrap();
    let signal = i32::from_le_bytes(buf);
    println!("disconnection signal {}", signal);
}
