=====
USAGE
=====

1. Create virtualenv

    $ ./create_env.sh

2. Run benchmark

    $ ./run_benchmark.sh {dev|qa1|qa2|live} {dummy|soft|medium|hard} service

    Where service is the url to quijote service,
    for example: http://dev-models.olx.com.ar/quijote

    Benchmarks available
      dummy:   100 items - 5,10 threads (util for testing)
      soft:   1000 items - 5,10,20,30,40,50 threads
      medium: 2000 items - 10,25,50,100,150,200 threads
      hard:   3000 items - 10,25,50,100,200,300 threads

    Use example:
    $ ./run_benchmark.sh dev dummy http://dev-models.olx.com.ar/quijote

3. Send the results_*.tar.gz file to Models Team.
