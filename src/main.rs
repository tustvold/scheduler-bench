use arrow::array::{Array, ArrayRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::async_reader::{FileStorage, ParquetRecordBatchStream};
use parquet::arrow::{ArrowReader, ParquetFileArrowReader, ParquetRecordBatchStreamBuilder};
use parquet::file::reader::{ChunkReader, SerializedFileReader};
use parquet::file::serialized_reader::SliceableCursor;
use statrs::statistics::{Data, Distribution, Max, Min, OrderStatistics};
use std::fmt::Display;
use std::fs::File;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tikv_jemallocator::Jemalloc;
use tokio::runtime::Runtime;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const DICT_10_REQUIRED_IDX: usize = 0;
const DICT_1000_REQUIRED_IDX: usize = 4;
const STRING_OPTIONAL_IDX: usize = 7;

fn main() {
    //let batch_sizes = [1024, 2048, 4096, 8192, 16384];
    let batch_sizes = [2048];
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .unwrap();

    for batch_size in batch_sizes {
        // bench(
        //     format_args!("sync_file_test ({})", batch_size),
        //     sync_file_test(batch_size),
        // );
        //
        // bench(
        //     format_args!("sync_mem_test ({})", batch_size),
        //     sync_mem_test(batch_size),
        // );
        //
        // let (f, handle) = par_sync_file_test(batch_size);
        // bench(format_args!("par_sync_file_test ({})", batch_size), f);
        // handle.join().unwrap();
        //
        bench(
            format_args!("tokio_sync_file_test ({})", batch_size),
            tokio_sync_file_test(batch_size, &runtime),
        );
        //
        // bench(
        //     format_args!("tokio_spawn_file_test ({})", batch_size),
        //     tokio_spawn_file_test(batch_size, &runtime),
        // );
        //
        // bench(
        //     format_args!("tokio_spawn_file_buffer_test ({})", batch_size),
        //     tokio_spawn_file_buffer_test(batch_size, &runtime),
        // );
        //
        // bench(
        //     format_args!("tokio_async_test ({})", batch_size),
        //     tokio_async_test(batch_size, &runtime),
        // );

        bench(
            format_args!("tokio_par_async_spawn_blocking_test ({})", batch_size),
            tokio_par_async_test(batch_size, &runtime, true),
        );

        bench(
            format_args!("tokio_par_async_blocking_test ({})", batch_size),
            tokio_par_async_test(batch_size, &runtime, false),
        );

        bench(
            format_args!("tokio_par_sync_test ({})", batch_size),
            tokio_par_sync_test(batch_size, &runtime),
        );
    }
}

fn bench(name: impl Display, mut f: impl FnMut()) {
    const SAMPLE_COUNT: usize = 100;
    let mut elapsed = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        let start = Instant::now();
        f();
        let duration = start.elapsed().as_secs_f64();
        elapsed.push(duration);
    }

    let mut data = Data::new(elapsed);

    println!(
        "{}: min: {:.4}s, max: {:.4}s, avg: {:.4}s, p95: {:.4}s",
        name,
        data.min(),
        data.max(),
        data.mean().unwrap(),
        data.percentile(95),
    );
}

fn sync_file_test(batch_size: usize) -> impl Fn() {
    let file = File::open("test.parquet").unwrap();
    move || sync_test(sync_reader(batch_size, file.try_clone().unwrap()))
}

fn sync_mem_test(batch_size: usize) -> impl Fn() {
    let data = Arc::new(std::fs::read("test.parquet").unwrap());
    move || {
        let cursor = SliceableCursor::new(Arc::clone(&data));
        sync_test(sync_reader(batch_size, cursor))
    }
}

fn sync_reader<R: 'static + ChunkReader>(batch_size: usize, reader: R) -> ParquetRecordBatchReader {
    record_reader(batch_size, &mut arrow_reader(reader))
}

fn arrow_reader<R: 'static + ChunkReader>(reader: R) -> ParquetFileArrowReader {
    let reader = SerializedFileReader::new(reader).unwrap();
    ParquetFileArrowReader::new(Arc::new(reader))
}

fn record_reader(
    batch_size: usize,
    reader: &mut ParquetFileArrowReader,
) -> ParquetRecordBatchReader {
    reader
        .get_record_reader_by_columns(
            vec![
                DICT_10_REQUIRED_IDX,
                DICT_1000_REQUIRED_IDX,
                STRING_OPTIONAL_IDX,
            ],
            batch_size,
        )
        .unwrap()
}

fn filter_batch(batch: RecordBatch) -> ArrayRef {
    let a = arrow::compute::eq_dyn_utf8_scalar(batch.column(0), "prefix#1").unwrap();
    let b = arrow::compute::eq_dyn_utf8_scalar(batch.column(1), "prefix#1").unwrap();
    let filter = arrow::compute::and(&a, &b).unwrap();

    arrow::compute::filter(batch.column(2), &filter).unwrap()
}

fn sync_test(reader: ParquetRecordBatchReader) {
    let mut count = 0;

    for maybe_batch in reader {
        let result = filter_batch(maybe_batch.unwrap());
        count += result.len();
    }

    assert_eq!(count, 210);
}

fn par_sync_file_test(batch_size: usize) -> (impl FnMut(), JoinHandle<()>) {
    let mut reader = arrow_reader(File::open("test.parquet").unwrap());

    let (t1_sender, t1_receiver) = std::sync::mpsc::channel();
    let (t2_sender, t2_receiver) = std::sync::mpsc::channel();

    // Reads data from parquet
    let join = std::thread::spawn(move || {
        while let Ok(_) = t1_receiver.recv() {
            let reader = record_reader(batch_size, &mut reader);
            for maybe_batch in reader {
                let batch = maybe_batch.unwrap();
                t2_sender.send(Some(batch)).unwrap();
            }
            t2_sender.send(None).unwrap();
        }
    });

    let bench = move || {
        t1_sender.send(()).unwrap();
        let mut count = 0;
        while let Ok(data) = t2_receiver.recv() {
            match data {
                Some(batch) => count += filter_batch(batch).len(),
                None => {
                    assert_eq!(count, 210);
                    return;
                }
            }
        }
    };

    (bench, join)
}

fn tokio_sync_file_test(batch_size: usize, runtime: &Runtime) -> impl FnMut() + '_ {
    let mut reader = arrow_reader(File::open("test.parquet").unwrap());

    move || {
        runtime.block_on(async {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(10);
            let reader = record_reader(batch_size, &mut reader);

            let task = tokio::task::spawn_blocking(move || {
                for maybe_batch in reader {
                    let batch = maybe_batch.unwrap();
                    sender.blocking_send(batch).unwrap();
                }
            });

            let mut count = 0;
            while let Some(batch) = receiver.recv().await {
                count += filter_batch(batch).len();
            }
            assert_eq!(count, 210);

            task.await.unwrap();
        })
    }
}

fn tokio_spawn_file_test(batch_size: usize, runtime: &Runtime) -> impl FnMut() + '_ {
    let mut reader = arrow_reader(File::open("test.parquet").unwrap());

    move || {
        runtime.block_on(async {
            let mut reader = record_reader(batch_size, &mut reader);

            let mut count = 0;
            loop {
                let task = tokio::task::spawn_blocking(move || {
                    let next = reader.next();
                    (next, reader)
                });

                let result = task.await.unwrap();
                reader = result.1;

                match result.0 {
                    Some(batch) => count += filter_batch(batch.unwrap()).len(),
                    None => break,
                }
            }

            assert_eq!(count, 210);
        })
    }
}

fn tokio_spawn_file_buffer_test(batch_size: usize, runtime: &Runtime) -> impl FnMut() + '_ {
    move || {
        runtime.block_on(async {
            let data = tokio::fs::read("test.parquet").await.unwrap();
            let reader = sync_reader(batch_size, SliceableCursor::new(data));
            sync_test(reader)
        })
    }
}

async fn async_reader(
    batch_size: usize,
    spawn_blocking: bool,
) -> ParquetRecordBatchStream<FileStorage> {
    let file = FileStorage::new(File::open("test.parquet").unwrap(), spawn_blocking);
    ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .with_batch_size(batch_size)
        .with_projection(vec![
            DICT_10_REQUIRED_IDX,
            DICT_1000_REQUIRED_IDX,
            STRING_OPTIONAL_IDX,
        ])
        .build()
        .await
        .unwrap()
}

fn tokio_async_test(
    batch_size: usize,
    runtime: &Runtime,
    spawn_blocking: bool,
) -> impl FnMut() + '_ {
    move || {
        runtime.block_on(async move {
            let mut reader = async_reader(batch_size, spawn_blocking).await;

            let mut count = 0;
            while let Some(maybe_batch) = reader.next().await {
                count += filter_batch(maybe_batch.unwrap()).len()
            }
            assert_eq!(count, 210);
        })
    }
}

fn tokio_par_async_test(
    batch_size: usize,
    runtime: &Runtime,
    spawn_blocking: bool,
) -> impl FnMut() + '_ {
    move || {
        runtime.block_on(async move {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(10);
            let t1 = tokio::task::spawn(async move {
                let mut reader_durations = Vec::with_capacity(1024);

                let start = Instant::now();
                let mut last_instant = start;

                let mut reader = async_reader(batch_size, spawn_blocking).await;
                while let Some(maybe_batch) = reader.next().await {
                    let a = Instant::now();
                    reader_durations.push(a.duration_since(last_instant));
                    last_instant = a;

                    sender.send(maybe_batch.unwrap()).await.unwrap()
                }

                let reader_total = reader_durations.iter().sum::<Duration>();
                let reader_max = reader_durations.iter().max().unwrap();
                let reader_min = reader_durations.iter().min().unwrap();

                println!(
                    "Read completed in {}, reader sum: {}, reader min: {}, reader max: {}",
                    start.elapsed().as_secs_f64(),
                    reader_total.as_secs_f64(),
                    reader_min.as_secs_f64(),
                    reader_max.as_secs_f64()
                );
            });

            let mut last_instant = Instant::now();
            let mut filter_count = 0;
            let mut max_delay = Duration::from_secs(0);
            let mut max_filter = Duration::from_secs(0);

            let mut count = 0;
            while let Some(batch) = receiver.recv().await {
                filter_count += 1;
                let a = Instant::now();

                count += filter_batch(batch).len();

                let b = Instant::now();
                max_delay = a.duration_since(last_instant);
                max_filter = b.duration_since(a);
                last_instant = b;
            }
            assert_eq!(count, 210);

            println!(
                "count: {}, max delay: {}s, max filter: {}s",
                filter_count,
                max_delay.as_secs_f64(),
                max_filter.as_secs_f64()
            );

            t1.await.unwrap();
        })
    }
}

fn tokio_par_sync_test(batch_size: usize, runtime: &Runtime) -> impl FnMut() + '_ {
    move || {
        runtime.block_on(async move {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(10);
            let t1 = tokio::task::spawn(async move {
                let file = File::open("test.parquet").unwrap();
                let reader = sync_reader(batch_size, file);
                for maybe_batch in reader {
                    sender.send(maybe_batch.unwrap()).await.unwrap()
                }
            });

            let mut count = 0;
            while let Some(batch) = receiver.recv().await {
                count += filter_batch(batch).len()
            }
            assert_eq!(count, 210);

            t1.await.unwrap();
        })
    }
}
