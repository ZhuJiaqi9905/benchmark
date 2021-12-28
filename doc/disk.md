# 文件IO的benchmark

## read file
顺序读取一个约1G到文件。每次读buf_len长度的内容到缓冲区中。

做round轮读取，得到读取的总文件大小和总时间，计算吞吐量MB/s。
- File::Read


buf_len | throughput
--------|-----------
1024 | 1233.026MB/s
2048 | 2150.412MB/s
3072 | 2770.663MB/s
4096 | 3395.353MB/s
5120 | 3672.376MB/s

- BufReader

buf_len | throughput
--------|-----------
1024 | 3387.283MB/s
2048 | 4313.264MB/s
3072 | 4317.124MB/s
4096 | 4293.708MB/s
5120 | 4057.761MB/s

结论：

- 读文件到带宽在4300MB/s左右
- BufReader的性能要优于直接调用File的read。优先使用BufReader读取文件