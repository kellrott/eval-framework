
Install
-------
```
sudo apt-get update
sudo apt-get install -y docker.io
sudo usermod -aG docker $USER
sudo pip install pika

git clone https://github.com/kellrott/eval-framework.git
```

Deploy Work Queue
```
docker run -d --hostname my-rabbit --name job-rabbit -p 5672:5672 -e RABBITMQ_DEFAULT_USER=user -e RABBITMQ_DEFAULT_PASS=password rabbitmq:3
```

