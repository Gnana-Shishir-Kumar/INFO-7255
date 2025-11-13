import os, json, pika

QUEUE = os.getenv("QUEUE", "plan-jobs")
AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")

def _channel():
    params = pika.URLParameters(AMQP_URL)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=f"{QUEUE}.dlq", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True, arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": f"{QUEUE}.dlq"
    })
    return conn, ch

def publish(job: dict):
    conn, ch = _channel()
    try:
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=json.dumps(job),
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
        )
    finally:
        conn.close()

