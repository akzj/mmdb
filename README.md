# mmdb

## 内存键值DB,数据存储在btree
* 支持日志持久化,快照防止数据丢失
* 支持ACID事务
* 基于btree Cow,MVCC支持并发写,发读取(并发读写互不堵塞对方)

