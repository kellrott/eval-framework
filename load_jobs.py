#!/usr/bin/env python

import argparser
from kafka import KafkaProducer


if __name__ == "__main__":
    parser = argparser.ArgumentParser()
    parser.add_argument("workflow")
    parser.add_argument("inputs")
    parser.add_argument("output")
    parser.add_argument("--id-field", default="id")
    parser.add_argument("--server", default='localhost:9092')
    
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.server)

    with open(args.workflow) as handle:
        workflow = yaml.load(handle.read())

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