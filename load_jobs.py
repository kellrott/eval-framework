#!/usr/bin/env python

import os
import json
import argparse
import subprocess
from kafka import KafkaProducer


def which(file):
    for path in os.environ["PATH"].split(":"):
        p = os.path.join(path, file)
        if os.path.exists(p):
            return p

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("workflow")
    parser.add_argument("inputs")
    parser.add_argument("output")
    parser.add_argument("--id-field", default="id")
    parser.add_argument("--server", default='localhost:9092')
    
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.server)

    with open(args.workflow) as handle:
        proc = subprocess.Popen([which('cwltool'), '--pack', args.workflow], stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        workflow = json.loads(stdout)

    with open(args.inputs) as handle:
        for line in handle:
            inputs = json.loads(line)
            output = os.path.join( args.output, inputs[args.id_field] )
            data = {
                "workflow" : workflow,
                "inputs" : inputs,
                "output" : output
            }            
            producer.send('cwl-jobs', json.dumps(data))


