# This command starts a RabbitMQ container with a management UI
# It will be named "rabbitmq" and will be accessible on our local machine.

docker run -d --hostname my-rabbit --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management