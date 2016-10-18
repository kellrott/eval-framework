#!/bin/bash

BDIR="$(cd `dirname $0`; pwd)"

nohup $BDIR/work_and_shutdown.sh $* > $BDIR/log.stdout 2> $BDIR/log.stderr 
