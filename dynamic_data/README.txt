Create virtualenv
-----------------

    $ ./create_env.sh


Run benchmark
-------------

 There are three modes: soft, medium and hard

 * soft: test from 50 to 200 simultaneous clients during 600 seconds each one
 * medium: test from 50 to 350 simultaneous clients during 600 seconds each one
 * hard: test from 50 to 500 simultaneous clients during 600 seconds each one 

    $ ./run_benchmark.sh soft http://quijote_hostname
    $ ./run_benchmark.sh medium http://quijote_hostname

    ALERT! To run the hard test reduce the default stack size used by a thread
           to 4096 or 2048:

    $ ulimit -s 4096
    $ ./run_benchmark.sh hard http://quijote_hostname

 (The last parameter is the Quijote url, without / at the end)


Finally collect the results and send us the benchmarks.tar.gz file
------------------------------------------------------------------

    $ ./collect_all_benchmarks.sh

