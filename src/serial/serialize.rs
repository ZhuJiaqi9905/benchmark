use std::{fs::File, io::Read, time::SystemTime};
use bytes::BufMut;
use bytes::BytesMut;
use bytes::Bytes;
pub fn bincode_serialize<T: serde::ser::Serialize>(data: &[u8], batch_size: usize) {
    println!("func start");
    let mut s_batch = 0;
    let mut e_batch = s_batch + batch_size;
    let start = SystemTime::now();
    let mut total_size = 0;
    // let mut buf = Vec::with_capacity(data.len());
    println!("test begin");
    let mut buf = BytesMut::with_capacity(1024);
    for _i in 0..(data.len() / batch_size) {
        // let serialized_data =
        //     bincode::serialize(&data[s_batch..e_batch]).expect("can not serialized");
        
        // bincode::serialize_into(&mut buf, &data[s_batch..e_batch]).unwrap();
        let d = &data[s_batch..e_batch];
        buf.put_u32_le(d.len() as u32);
        buf.put(&d[..]);
        
        let ser_buf = buf.split();
        let s = ser_buf.freeze();
        let x = s.len();
        // buf.extend_from_slice(&d[..]);
        // buf.extend_from_slice(extend)
        // let ser_buf = buf.split();
        // println!("len: {}, capacity: {}", ser_buf.len(), ser_buf.capacity());
        // break;
        // buf.extend_from_slice(&d.len().to_le_bytes());
        // buf.extend_from_slice(&d[..]);
        s_batch += batch_size;
        e_batch += batch_size;
        total_size += x;
    }
    let end = SystemTime::now();
    let duration = (end.duration_since(start).unwrap().as_micros() as f64) / 1e6;
    let total_size = total_size as f64 / (1024f64 * 1024f64);
    let throughput = total_size / duration;
    println!(
        "duration: {}s, bincode serialize throughput: {:.3}MB/s",
        duration, throughput
    );
}

pub fn test_serialize(in_path: &str, batch_size: usize) {
    let mut file = File::open(in_path).expect("Unable to open input file");
    let mut data = Vec::<u8>::with_capacity(10240);
    file.read_to_end(&mut data).unwrap();
    bincode_serialize::<u8>(&data, batch_size);
}
