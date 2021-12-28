use disk::file_rw::bufread_throughput;

use crate::disk::file_rw::read_throughput;

mod disk;
fn main() {
    read_throughput("data/bigfile.log", 5120);
    bufread_throughput("data/bigfile.log", 5120);
}
