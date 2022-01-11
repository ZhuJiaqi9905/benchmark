mod r_client;
mod r_server;
use std::{
    fs::File,
    io::{BufReader, Seek},
};

use r_server::Rserver;
use r_client::Rclient;
pub fn test_rserver(listen_addr: &str, in_path: &str) {
    let stream = Rserver::listen_one(listen_addr);

    let mut rserver = Rserver::new(stream, in_path, 1024);
    let res = rserver.wait_for_disconnect().unwrap();
    if res == true {
        println!("disconnect protocol success");
    } else {
        println!("disconnect protocol fail");
    }
}
pub fn test_rclient(dst: &str){
    let mut rclient = Rclient::connect(dst, 1073733000 , 1024);
    rclient.read_data(10240);
    rclient.disconnect();
}