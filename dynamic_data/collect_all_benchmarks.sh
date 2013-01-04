#!/bin/bash
FILE=benchmarks.tar.gz
rm $FILE 2>/dev/null
tar czf $FILE dynamic-data*
