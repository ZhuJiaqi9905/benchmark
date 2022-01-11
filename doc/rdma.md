# RDMA的benchmark

## RDMA Read

server把数据从文件中读取到内存。client端生成一个等大的buffer，通过rdma read操作，每次只能读取batch_size大小的数据。client端一直读取，直到把数据读完。

- 设置qp的sq_sig_all为0，使得大部分read request不产生cqe。
- 记录wqe的个数，当cq要满了时，放置一个产生cqe的wqe。

batch_size | throughput
----- | -----
