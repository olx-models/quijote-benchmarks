#!/bin/bash

show_help ()
{
    echo "Usage: $0 <dev|qa1|qa2|live> <dummy|soft|medium|hard> <quijote-url> [output-filename]"
    echo
    echo "Example: $0 dev dummy http://dev-models.olx.com.ar/quijote"
    echo
    echo "Benchmarks"
    echo "    dummy:   duration  10s - freq 5,10,15/s (util for testing)"
    echo "    soft:    duration  60s - freq 10,15,20,25/s"
    echo "    medium:  duration 120s - freq 10,20,30,40,50/s"
    echo "    hard:    duration 120s - freq 20,40,60,80,100/s"
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
    FILENAME=${4:-freq_$2}
fi



echo "Running bench $LEVEL against $SERVICE.."
case $LEVEL in
    dummy)
        python freq.py --env=$ENVIRONMENT --duration=10 \
                       --freq=5 --freq=10 --freq=15 \
                       --service=$SERVICE ;;
    soft)
        python freq.py --env=$ENVIRONMENT --duration=60 \
                       --freq=10 --freq=15 --freq=20 --freq=25 \
                       --service=$SERVICE ;;
    medium)
        python freq.py --env=$ENVIRONMENT --duration=120 \
                       --freq=10 --freq=20 --freq=30 --freq=40 --freq=50 \
                       --service=$SERVICE ;;
    hard)
        python freq.py --env=$ENVIRONMENT --duration=120 \
                       --freq=20 --freq=40 --freq=60 --freq=80 --freq=100 \
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
