#!/bin/bash

print_help () {
    echo "Usage $0 <bench-level> <quijote-url>"
    echo "- bench level: soft, medium or hard"
    echo "- quijote url: http://dev-models.olx.com.ar/quijote  (without / at the end)"
}

if [ -z $1 ] || [ -z $2 ]; then
    print_help
    exit
fi

if [ $1 != "soft" ] && [ $1 != "medium" ] && [ $1 != "hard" ]; then
    print_help
    exit
fi


if [ ! -d "env" ]; then
    ./create_env.sh
fi

if [ -d "env" ]; then
    source env/bin/activate

    case $1 in
        soft)
            CYCLES="10:20:30:40:50" ;;
        medium)
            CYCLES="50:100:150:200:250:300:350" ;;
        hard)
            CYCLES="50:100:150:200:250:300:350:400:450:500" ;;
    esac

    mkdir tmp 2>/dev/null

    fl-run-bench --cycles=$CYCLES --url=$2 test_item_soa.py ItemSoa.test_get
    fl-build-report --html tmp/bench-item-soa.xml
    deactivate
fi
