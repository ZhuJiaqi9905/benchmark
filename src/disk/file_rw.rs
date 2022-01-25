use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    time::{Duration, SystemTime},
};
pub fn read_throughput(in_path: &str, buf_len: usize) {
    let round = 10;
    let mut total_size = 0;
    let mut total_duration = 0f64;
    for _i in 0..round {
        let mut in_file = File::open(in_path).expect("Unable to open input file");
        let mut in_buf = vec![0u8; buf_len];
        let mut file_size = 0;
        let read_start = SystemTime::now();
        while let Ok(len) = in_file.read(&mut in_buf) {
            file_size += len;
            if len == 0 {
                break;
            }
        }
        let read_end = SystemTime::now();
        let duration = read_end.duration_since(read_start).unwrap().as_secs_f64();
        total_duration += duration;
        total_size += file_size;
    }
    let total_size = total_size as f64 / (1024f64 * 1024f64); //MB
    let throughput = total_size / total_duration;
    println!("read file throughput: {:.3}MB/s", throughput);
}

pub fn bufread_throughput(in_path: &str, buf_len: usize) {
    let round = 10;
    let mut total_size = 0;
    let mut total_duration = 0f64;
    for _i in 0..round {
        let mut in_file = BufReader::new(File::open(in_path).expect("Unable to open input file"));
        let mut in_buf = vec![0u8; buf_len];
        let mut file_size = 0;
        let read_start = SystemTime::now();
        while let Ok(len) = in_file.read(&mut in_buf) {
            file_size += len;
            if len == 0 {
                break;
            }
        }
        let read_end = SystemTime::now();
        let duration = read_end.duration_since(read_start).unwrap().as_secs_f64();
        total_duration += duration;
        total_size += file_size;
    }
    let total_size = total_size as f64 / (1024f64 * 1024f64); //MB
    let throughput = total_size / total_duration;
    println!("read file throughput: {:.3}MB/s", throughput);
}

pub fn bufwrite_throughput(in_path: &str, out_path: &str, buf_len: usize) {
    let mut in_file = File::open(in_path).unwrap();
    let mut data = Vec::new();
    in_file.read_to_end(&mut data).unwrap();
    let total_size = data.len();
    let mut s = 0;
    let mut out_file = BufWriter::new(File::create(out_path).unwrap());
    let start = SystemTime::now();
    for _i in 0..(total_size / buf_len) {
        out_file.write_all(&data[s..(s + buf_len)]).unwrap();
        s += buf_len;
    }
    if s < total_size {
        out_file.write_all(&data[s..]).unwrap();
    }
    out_file.flush().unwrap();
    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap().as_micros() as f64  / 1e6;
    print!("total size: {}", total_size);
    let total_size = total_size as f64 / (1024f64 * 1024f64);
    let throughput = total_size / duration;
    println!("rdma write throughput: {:.3}MB/s", throughput);
}
