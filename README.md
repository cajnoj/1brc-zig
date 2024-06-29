# A Zig 1BRC Optimized Implementation

## Benchmark
Tested on a AWS EC2 c7a.8xlarge instance (32 AMD EPYC 9R14 cores) running Amazon Linux.

```
$ uname -a
Linux [...] 6.1.92-99.174.amzn2023.x86_64 #1 SMP PREEMPT_DYNAMIC Tue Jun  4 15:43:46 UTC 2024 x86_64 x86_64 x86_64 GNU/Linux

$ hyperfine --warmup 1 "zig-out/bin/z1brc > result.json"
Benchmark 1: zig-out/bin/z1brc > result.json
  Time (mean ± σ):      2.347 s ±  0.023 s    [User: 72.107 s, System: 0.973 s]
  Range (min … max):    2.316 s …  2.387 s    10 runs

```

## Optimizations
This program makes use of the following Zig features:

1. SIMD processing for min/max/sum calculations
1. Multi-threading (one thread per core)
1. String-based HashMap for per-thread and final results
1. Unsafe arithmetics
1. Opitmized (unsafe) float arithmetics
1. A buffered reader
1. Mutex for final result aggregation
1. Arena allocator per-thread for less cleanup overhead
1. Custom float parsing - the float parser reads directly from the buffered reader, thus preventing a redundant memory scan and working around the safe (and therefore much slower) standard float parsing function
