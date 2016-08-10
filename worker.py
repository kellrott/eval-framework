#!/usr/bin/env python


import argparser
import subprocess
import tempfile
import yaml
import shutil
from kafka import KafkaConsumer

from service import Service

class RunnerService(Service):
    def __init__(self, cmd_args, *args, **kwargs):
        super(MyService, self).__init__(*args, **kwargs)
        self.args = cmd_args
    
    def run(self):
        run_main(self.args)

def run_main(args):
    consumer = KafkaConsumer('cwl-jobs', 
        bootstrap_servers=args.server,
        consumer_timeout_ms=3000,
        group_id=args.group)
    for msg in consumer:
        data = json.loads(msg)
        workflow = data['workflow']
        inputs = data['inputs']
        output = data['output']
        
        tdir = tempfile.mkdtemp(dir=args.workdir, prefix="cwl_workqueue")
        with open(os.path.join(tdir, "workflow.cwl"), "w") as handle:
            handle.write(yaml.dump(workflow))
        
        with open(os.path.join(tdir, "inputs.json"), "w") as handle:
            handle.write(json.dumps(inputs))
            
        subprocess.call([
            "./cwl-gs-tool",
            os.path.join(tdir, "workflow.cwl"),
            os.path.join(tdir, "inputs.json"),
            output])
        
        shutil.rmtree(tdir)

if __name__ == "__main__":
    parser = argparser.ArgumentParser()
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
    