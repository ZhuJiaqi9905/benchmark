# RDMA的benchmark

## RDMA Read

server把数据从文件中读取到内存。client端生成一个等大的buffer，通过rdma read操作，每次只能读取batch_size大小的数据。client端一直读取，直到把数据读完。

* 设置qp的sq_sig_all为0，使得大部分read request不产生cqe。
* 记录wqe的个数，当cq要满了时，放置一个产生cqe的wqe。

batch_size(B) | throughput(MB/s)
----- | -----
1024 | 409.291
10240 | 2339.829
102400 | 5286.479
204800 | 5786.241
1024000 | 6365.728
4096000 | 6474.686
10240000 | 6363.948

可以看到RDMA read带宽就是6GB/s左右了，而且这种read是远端准备好自己的数据，然后本地直接read数据，才能达到这样的吞吐量。如果每次仅读取小数据，是无法跑满带宽的。

```
dl24
cargo run --release -- --bench rdma --rdma read_server

dl25
cargo run --release -- --bench rdma --rdma read_client
```