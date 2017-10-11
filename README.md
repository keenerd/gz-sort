More details at [http://kmkeen.com/gz-sort/](http://kmkeen.com/gz-sort/)

perform a merge sort over a multi-GB gz compressed file


### Quickstart
Needs the zlib headers and probably only builds on GNU/Linux.
```
$ apt-get install build-essential libz-dev
$ git clone https://github.com/keenerd/gz-sort
$ cd gz-sort
$ make
$ ./gz-sort -h
```

### Usage
    use: gz-sort [-u] [-S n] [-P n] source.gz dest.gz

    options:
       -h: help
       -u: unique
       -S n: size of presort, supports k/M/G suffix
             a traditional in-memory sort (default n=1M)
       -P n: use multiple threads (experimental, default disabled)
       -T: pass through (debugging/benchmarks)

    estimating run time, crudely:
        time gzip -dc data.gz | gzip > /dev/null
        unthreaded: seconds * entropy * (log2(uncompressed_size/S)+2)
        (where 'entropy' is a fudge-factor between 1.5 for an
        already sorted file and 3 for a shuffled file)
        S and P are the corresponding settings
        multithreaded: maybe unthreaded/sqrt(P) ?

    estimated disk use:
        2x source.gz


### Minimum requirements to sort a terabyte:

* 4MB ram  (yes, megabyte)
* free disk space equal to the twice the compressed source.gz


### Known bugs to fix

Email me if you are using gz-sort and any of these omissions are causing you trouble.  For that matter, email me if you find something not on this  list too.

* Does not build on non-gnu systems.
* Sqrt(threads) is a terrible ratio.
* No support for uncompressed stdin streams.
* Breaks if a line is longer than the buffer size.
* Lacks all error handling.
* Ugly code with lots of ways to refactor.
* Output could use predictable flushes.


### Performance tweaks to try

* Profile!
* Parallelize the final n-way merge.  This will require adding IPC.
* Filter unique lines during the earlier passes.
* Try out zlib-ng, about half of cpu time is spent on (un)gzipping.
* Improve memory estimation, it lowballs and that hurts the presort.
* Byte-based seeking instead of line-counting.


