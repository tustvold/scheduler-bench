use arrow::array::{Array, ArrayRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader, ParquetRecordBatchStreamBuilder};
use parquet::file::reader::{ChunkReader, SerializedFileReader};
use parquet::file::serialized_reader::SliceableCursor;
use std::fmt::Display;
use std::fs::File;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;

const DICT_10_REQUIRED_IDX: usize = 0;
const DICT_1000_REQUIRED_IDX: usize = 4;
const STRING_OPTIONAL_IDX: usize = 7;

fn main() {
    let batch_sizes = [1024, 2048, 4096, 8192, 16384];
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();

    for batch_size in batch_sizes {
        bench(
            format_args!("sync_file_test ({})", batch_size),
            sync_file_test(batch_size),
        );

        bench(
            format_args!("sync_mem_test ({})", batch_size),
            sync_mem_test(batch_size),
        );

        let (f, handle) = par_sync_file_test(batch_size);
        bench(format_args!("par_sync_file_test ({})", batch_size), f);
        handle.join().unwrap();

        bench(
            format_args!("tokio_sync_file_test ({})", batch_size),
            tokio_sync_file_test(batch_size, runtime.handle().clone()),
        );

        bench(
            format_args!("tokio_spawn_file_test ({})", batch_size),
            tokio_spawn_file_test(batch_size, runtime.handle().clone()),
        );

        bench(
            format_args!("tokio_spawn_file_buffer_test ({})", batch_size),
            tokio_spawn_file_buffer_test(batch_size, runtime.handle().clone()),
        );

        bench(
            format_args!("tokio_async_test ({})", batch_size),
            tokio_async_test(batch_size, runtime.handle().clone()),
        );

        bench(
            format_args!("tokio_par_async_test ({})", batch_size),
            tokio_par_async_test(batch_size, runtime.handle().clone()),
        );
    }
}

fn bench(name: impl Display, mut f: impl FnMut()) {
    const SAMPLE_COUNT: usize = 100;
    let mut elapsed = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        let start = Instant::now();
        f();
        elapsed.push(start.elapsed());
    }

    let min = elapsed.iter().min().unwrap();
    let max = elapsed.iter().max().unwrap();
    let sum = elapsed.iter().sum::<Duration>();

    println!(
        "{}: min: {:.4}s, max: {:.4}s, avg: {:.4}s",
        name,
        min.as_secs_f64(),
        max.as_secs_f64(),
        sum.as_secs_f64() / elapsed.len() as f64
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

fn tokio_sync_file_test(batch_size: usize, handle: Handle) -> impl FnMut() {
    let mut reader = arrow_reader(File::open("test.parquet").unwrap());

    move || {
        handle.block_on(async {
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

fn tokio_spawn_file_test(batch_size: usize, handle: Handle) -> impl FnMut() {
    let mut reader = arrow_reader(File::open("test.parquet").unwrap());

    move || {
        handle.block_on(async {
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

fn tokio_spawn_file_buffer_test(batch_size: usize, handle: Handle) -> impl FnMut() {
    move || {
        handle.block_on(async {
            let data = tokio::fs::read("test.parquet").await.unwrap();
            let reader = sync_reader(batch_size, SliceableCursor::new(data));
            sync_test(reader)
        })
    }
}

async fn async_reader(batch_size: usize) -> ParquetRecordBatchStream<tokio::fs::File> {
    let file = tokio::fs::File::open("test.parquet").await.unwrap();
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
        .unwrap()
}

fn tokio_async_test(batch_size: usize, handle: Handle) -> impl FnMut() {
    move || {
        handle.block_on(async move {
            let mut reader = async_reader(batch_size).await;

            let mut count = 0;
            while let Some(maybe_batch) = reader.next().await {
                count += filter_batch(maybe_batch.unwrap()).len()
            }
            assert_eq!(count, 210);
        })
    }
}

fn tokio_par_async_test(batch_size: usize, handle: Handle) -> impl FnMut() {
    move || {
        handle.block_on(async move {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(10);
            let t1 = tokio::task::spawn(async move {
                let mut reader = async_reader(batch_size).await;
                while let Some(maybe_batch) = reader.next().await {
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
