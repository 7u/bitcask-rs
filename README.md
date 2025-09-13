# bitcask-rs(WIP)

一个用 Rust 编写的 **Bitcask 键值存储** 实验性实现，主要探索 **并发模型**、**Tokio 集成** 和 **追加写存储设计**。  

---

## 功能

- [x] 追加写 (append-only) 的 Active File  
- [x] 基本的 `put/get/delete` 操作  
- [x] 内存中的 keydir，基于时间戳的冲突解决  
- [x] 文件轮转 (active → readonly)  
- [ ] Hint file 支持  
- [ ] Compaction / merge  
- [ ] 崩溃恢复  

---

## 并发模型

- **写入 (Actor model)**  
  - 前台任务通过 `mpsc` channel 发送请求  
  - 后台任务独占 ActiveFile，执行写入  
  - keydir 只在写入确认后更新  

- **Flush 策略**  
  - 后台周期性 flush  
  - `close()` 时执行最终 flush + `sync_all`  

- **文件轮转 (rotation)**  
  - 老文件的 `sync_all`、`set_permissions` 等操作交给后台处理  
  - 需要保证轮转过程不会并发冲突  

- **读取 (read)**  
  - 设计中：是每个 ActiveFile 用 async `BufReader`，还是集中用 sync FileCache  

---

## 设计难点
我目前卡在以下这些设计取舍上：

1. **写入原子性**  
   - 复合操作 = “追加写 + 更新索引” 不是原子操作  
   - 在 Bitcask 的 *append-only + timestamp-wins* 语义下，这是否可接受？  

2. **文件轮转的并发**  
   - `rotate()` 包含异步操作，必须避免竞争条件  
   - 使用 `RwLock` + 单线程 Actor，是否足够？  

3. **Tokio runtime 集成方式**  
   - 方案 A：单线程 runtime，多个 client task 并发  
   - 方案 B：顺序执行的 actor-style 驱动 task  


