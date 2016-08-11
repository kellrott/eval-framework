#!/usr/bin/env python

import os
import argparse
import subprocess
import tempfile
import json
import shutil
from kafka import KafkaConsumer

from service import Service

BASE_DIR = os.path.dirname(__file__)

class RunnerService(Service):
    def __init__(self, cmd_args, *args, **kwargs):
        super(MyService, self).__init__(*args, **kwargs)
        self.args = cmd_args
    
    def run(self):
        run_main(self.args)

def run_main(args):
    if not os.path.exists(args.workdir):
        os.mkdir(args.workdir)
    consumer = KafkaConsumer('cwl-jobs', 
        bootstrap_servers=args.server,
                             auto_offset_reset="earliest",
        consumer_timeout_ms=3000,
        group_id=args.group)
    for msg in consumer:
        print msg
        data = json.loads(msg.value)
        workflow = data['workflow']
        inputs = data['inputs']
        output = data['output']
        
        tdir = tempfile.mkdtemp(dir=args.workdir, prefix="cwl_workqueue")
        with open(os.path.join(tdir, "workflow.cwl"), "w") as handle:
            handle.write(json.dumps(workflow))
        
        with open(os.path.join(tdir, "inputs.json"), "w") as handle:
            handle.write(json.dumps(inputs))
            
        subprocess.call([
            os.path.join(BASE_DIR, "cwl-gs-tool"),
            "--clear-cache",
            os.path.join(tdir, "workflow.cwl#main"),
            os.path.join(tdir, "inputs.json"),
            output])
        
        shutil.rmtree(tdir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("server")
    parser.add_argument("--workdir", default="cwl-workers")
    parser.add_argument("--group", default="cwl-workers")
    parser.add_argument("--shutdown", action="store_true", default=False)
    parser.add_argument("--daemon", action="store_true", default=False)
    
    args = parser.parse_args()
    
    if args.daemon:
        service = RunnerService(args, 'cwl_job_runner', pid_dir='/tmp')
        service.start()
    else:
        run_main(args)

    if args.shutdown:
        subprocess.check_call(["sudo", "shutdown", "now"])
    
