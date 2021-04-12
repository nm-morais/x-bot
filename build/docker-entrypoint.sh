#!/bin/sh

set -e

echo "all args: $@"
echo "Bootstraping TC, args: $1 $2 $3 $4 $5"
bash /setupTc.sh $1 $2 $3 $4 $5

echo "Bootstraping XBot"
shift 5
echo "xbot args: $@"
./go/bin/XBot "$@"