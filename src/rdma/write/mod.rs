use self::{w_server::Wserver, w_client::Wclient};

mod w_client;
mod w_server;

pub fn test_wserver(listen_addr: &str, in_path: &str) {
    let stream = Wserver::listen_one(listen_addr);

    let mut wserver = Wserver::new(stream, in_path, 1024);
    wserver.write_data(1024*1024);
    wserver.disconnect();
    
}
pub fn test_wclient(dst: &str){
    let mut wclient = Wclient::connect(dst, 1024);
    wclient.wait_for_disconnect();
}