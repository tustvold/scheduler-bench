# Scheduler Bench

Runs the query using different scheduling techniques and batch sizes

```sql
select string_optional from t where dict_10_required = 'prefix#1' and dict_1000_required = 'prefix#1';
```


## Setup:
1. Install git-lfs
2. Run `git lfs pull` to pull files (not needed if LFS was installed prior to checking out the repo)
3. Run with `cargo run --release`


## Example output:

```shell
$ cargo run --release
cargo run --release
    Finished release [optimized] target(s) in 0.07s
     Running `target/release/scheduler-bench`
sync_file_test (1024): min: 0.0968s, max: 0.1207s, avg: 0.1027s
sync_mem_test (1024): min: 0.0941s, max: 0.1220s, avg: 0.0996s
par_sync_file_test (1024): min: 0.0830s, max: 0.1085s, avg: 0.0875s
tokio_sync_file_test (1024): min: 0.0835s, max: 0.1080s, avg: 0.0917s
tokio_spawn_file_test (1024): min: 0.1341s, max: 0.1747s, avg: 0.1460s
tokio_spawn_file_buffer_test (1024): min: 0.1478s, max: 0.2100s, avg: 0.1600s
tokio_async_test (1024): min: 0.1431s, max: 0.1804s, avg: 0.1526s
tokio_par_async_test (1024): min: 0.1352s, max: 0.1612s, avg: 0.1465s
tokio_par_sync_test (1024): min: 0.0880s, max: 0.1382s, avg: 0.1071s
sync_file_test (2048): min: 0.0948s, max: 0.1768s, avg: 0.1318s
sync_mem_test (2048): min: 0.0916s, max: 0.1666s, avg: 0.1206s
par_sync_file_test (2048): min: 0.0794s, max: 0.1630s, avg: 0.1145s
tokio_sync_file_test (2048): min: 0.1023s, max: 0.2510s, avg: 0.1902s
tokio_spawn_file_test (2048): min: 0.2702s, max: 0.4230s, avg: 0.3572s
tokio_spawn_file_buffer_test (2048): min: 0.2147s, max: 0.3401s, avg: 0.2680s
tokio_async_test (2048): min: 0.2072s, max: 0.3323s, avg: 0.2596s
tokio_par_async_test (2048): min: 0.1819s, max: 0.2990s, avg: 0.2395s
tokio_par_sync_test (2048): min: 0.1382s, max: 0.2506s, avg: 0.1960s
sync_file_test (4096): min: 0.1420s, max: 0.2399s, avg: 0.1813s
sync_mem_test (4096): min: 0.1311s, max: 0.2399s, avg: 0.1782s
par_sync_file_test (4096): min: 0.1079s, max: 0.1974s, avg: 0.1534s
tokio_sync_file_test (4096): min: 0.1147s, max: 0.2118s, avg: 0.1638s
tokio_spawn_file_test (4096): min: 0.2236s, max: 0.3454s, avg: 0.2782s
tokio_spawn_file_buffer_test (4096): min: 0.1838s, max: 0.2941s, avg: 0.2340s
tokio_async_test (4096): min: 0.1808s, max: 0.2900s, avg: 0.2255s
tokio_par_async_test (4096): min: 0.1526s, max: 0.2556s, avg: 0.2048s
tokio_par_sync_test (4096): min: 0.1141s, max: 0.2171s, avg: 0.1619s
sync_file_test (8192): min: 0.1505s, max: 0.3689s, avg: 0.2261s
sync_mem_test (8192): min: 0.1558s, max: 0.4034s, avg: 0.2375s
par_sync_file_test (8192): min: 0.1149s, max: 0.2036s, avg: 0.1571s
tokio_sync_file_test (8192): min: 0.1135s, max: 0.2022s, avg: 0.1614s
tokio_spawn_file_test (8192): min: 0.3430s, max: 0.5689s, avg: 0.4529s
tokio_spawn_file_buffer_test (8192): min: 0.1879s, max: 0.3810s, avg: 0.2620s
tokio_async_test (8192): min: 0.1600s, max: 2.4110s, avg: 0.3616s
tokio_par_async_test (8192): min: 0.1781s, max: 0.4306s, avg: 0.2548s
tokio_par_sync_test (8192): min: 0.1223s, max: 0.3303s, avg: 0.2035s
sync_file_test (16384): min: 0.1890s, max: 0.4439s, avg: 0.3216s
sync_mem_test (16384): min: 0.1603s, max: 0.4605s, avg: 0.2934s
par_sync_file_test (16384): min: 0.1184s, max: 0.3450s, avg: 0.2003s
tokio_sync_file_test (16384): min: 0.1197s, max: 0.3192s, avg: 0.1841s
tokio_spawn_file_test (16384): min: 0.2144s, max: 0.5118s, avg: 0.3662s
tokio_spawn_file_buffer_test (16384): min: 0.2080s, max: 0.5335s, avg: 0.3274s
tokio_async_test (16384): min: 0.1867s, max: 0.4569s, avg: 0.2526s
tokio_par_async_test (16384): min: 0.1711s, max: 0.2684s, avg: 0.2118s
tokio_par_sync_test (16384): min: 0.0868s, max: 0.2195s, avg: 0.1579s
```
