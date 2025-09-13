use std::io;

mod config;
mod storage;
mod utils;

use config::BitCaskConfig;
use storage::bitcask_impl::BitCaskHandle;
#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();
    // info!("🚀 Tokio 运行时已启动");

    // // 示例异步任务
    // let task1 = tokio::spawn(async {
    //     info!("  子任务 1 开始");
    //     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    //     info!("  子任务 1 结束");
    // });
    // let task2 = tokio::spawn(async {
    //     info!("  子任务 2 开始");
    //     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    //     info!("  子任务 2 结束");
    // });

    // // 主任务
    // info!("主任务执行中...");
    // tokio::join!(task1, task2);
    // tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    // info!("主任务结束");

    let mut handle = BitCaskHandle::<BitCaskConfig>::open("./data").await?;
    handle.put(b"key", b"value").await?;
    let value = handle.get(b"key").await;
    println!("{value:?}");

    Ok(())
}
