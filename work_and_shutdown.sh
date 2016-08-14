#!/bin/bash

BDIR="$(cd `dirname $0`; pwd)"
$BDIR/worker $*
sudo shutdown now

