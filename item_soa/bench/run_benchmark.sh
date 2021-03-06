#!/bin/bash

show_help ()
{
    echo "Usage: $0 <dev|qa1|qa2|live> <dummy|soft|medium|hard> <quijote-url> [output-filename]"
    echo
    echo "Example: $0 dev dummy http://dev-models.olx.com.ar/quijote"
    echo
    echo "Benchmarks"
    echo "    dummy:   100 items - 5,10 threads (util for testing)"
    echo "    soft:   1000 items - 5,10,20,30,40,50 threads"
    echo "    medium: 2000 items - 10,25,50,100,150,200 threads"
    echo "    hard:   3000 items - 10,25,50,100,200,300 threads"
    exit 1
}

if [ $# -lt 3 ]
then
    show_help
else
    case $1 in
        dev|qa1|qa2|live) ENVIRONMENT=$1 ;;
        *) show_help ;;
    esac
    case $2 in
        dummy|soft|medium|hard) LEVEL=$2 ;;
        *) show_help ;;
    esac
    SERVICE=$3
    FILENAME=${4:-bench_$2}
fi



echo "Running bench $LEVEL against $SERVICE.."
case $LEVEL in
    dummy)
        python bench.py --env=$ENVIRONMENT --items=20 \
                        --threads=5 --threads=10 \
                        --service=$SERVICE ;;
    soft)
        python bench.py --env=$ENVIRONMENT --items=1000 \
                        --threads=5 --threads=10 --threads=20 \
                        --threads=30 --threads=40 --threads=50 \
                        --service=$SERVICE ;;
    medium)
        python bench.py --env=$ENVIRONMENT --items=2000 \
                        --threads=10 --threads=25 --threads=50 \
                        --threads=100 --threads=150 --threads=200 \
                        --service=$SERVICE ;;
    hard)
        python bench.py --env=$ENVIRONMENT --items=3000 \
                        --threads=10 --threads=25 --threads=50 \
                        --threads=100 --threads=200 --threads=300 \
                        --service=$SERVICE ;;
esac

if [ $? -eq 0 ]
then
    echo "Collecting data.."
    DATETIME=`date +%Y%m%dT%H%M%S`
    TGZ_FILE="${FILENAME}_${DATETIME}.tar.gz"
    tar czvf $TGZ_FILE *.log *.csv
    echo "Done!"
    echo "Please send $TGZ_FILE to Models Team"
fi

# Cleanup
rm *.log *.csv *.pyc 2>/dev/null
