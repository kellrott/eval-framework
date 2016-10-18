package main

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func ConsumerTimed(ch *amqp.Channel, queue string, autoAck bool, clientTimeout time.Duration) chan amqp.Delivery {

	out := make(chan amqp.Delivery, 1)

	go func() {
		var running bool = true
		lastMessage := time.Now()
		for running {
			msg, ok, _ := ch.Get(queue, autoAck)
			if ok {
				lastMessage = time.Now()
				out <- msg
			} else {
				curTime := time.Now()
				if curTime.Sub(lastMessage) > clientTimeout {
					close(out)
					running = false
				} else {
					time.Sleep(1 * time.Second)
				}
			}
		}
	}()

	return out
}

func main() {
	fmt.Println("Starting Worker")

	BASE_DIR, _ := filepath.Abs(path.Dir(os.Args[0]))
	TOOL_RUNNER := path.Join(BASE_DIR, "cwl-gs-tool")

	conn, err := amqp.Dial(os.Args[1])
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	for msg := range ConsumerTimed(ch, "cwl-jobs", true, 5*time.Second) {
		var data map[string]interface{}
		err := json.Unmarshal(msg.Body, &data)
		if err == nil {
			workflow, is_workflow := data["workflow"]
			workflow_url, is_workflow_url := data["workflow_url"]
			inputs := data["inputs"]
			output := data["output"].(string)

			tdir, _ := ioutil.TempDir("./", "cwl_workqueue")
			inputs_json, _ := json.Marshal(inputs)
			inputs_path := path.Join(tdir, "inputs.json")
			ioutil.WriteFile(inputs_path, inputs_json, 0600)

			var cmd *exec.Cmd
			if is_workflow {
				workflow_json, _ := json.Marshal(workflow)
				workflow_path := path.Join(tdir, "workflow.cwl")
				ioutil.WriteFile(workflow_path, workflow_json, 0600)
				cmd = exec.Command(TOOL_RUNNER, "--clear-cache", workflow_path, inputs_path, output)
			} else if is_workflow_url {
				cmd = exec.Command(TOOL_RUNNER, "--clear-cache", "--gs-workflow", workflow_url.(string), inputs_path, output)
			}
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.Run()
			os.RemoveAll(tdir)

		}
	}

}
