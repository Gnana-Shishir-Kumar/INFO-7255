import os, json, time
import pika
from elasticsearch import Elasticsearch, ConnectionError as ESConnError, TransportError
from services.elasticsearch_service import (
    index_plan as es_index,
    patch_plan as es_patch,
    delete_plan_from_index as es_delete,
    ensure_index,
)

ES = Elasticsearch(
    os.getenv("ES_URL", "http://localhost:9200"),
    request_timeout=10,
    retry_on_timeout=True,
)

ALIAS = os.getenv("ALIAS", "plan").strip()

QUEUE = os.getenv("QUEUE", "plan-jobs")
AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")
MAX_ATTEMPTS = 5

def main():
    ensure_index()
    params = pika.URLParameters(AMQP_URL)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_declare(queue=f"{QUEUE}.dlq", durable=True)
    ch.basic_qos(prefetch_count=1)

    def republish(job):
        ch.basic_publish(exchange="", routing_key=QUEUE,
                         body=json.dumps(job),
                         properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))

    def to_dlq(job, err):
        ch.basic_publish(exchange="", routing_key=f"{QUEUE}.dlq",
                         body=json.dumps({"error": str(err), "job": job}),
                         properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))

    def on_msg(ch_, method, props, body):
        job = json.loads(body)
        try:
            t = job.get("type")
            if t == "index":
                es_index(job["doc"])
            elif t == "patch":
                es_patch(job["id"], job["doc"])
            elif t == "delete":
                es_delete(job["id"])
            else:
                raise ValueError(f"Unknown job type {t}")
            ch_.basic_ack(delivery_tag=method.delivery_tag)
        except (ESConnError, TransportError) as e:
            job["attempt"] = job.get("attempt", 0) + 1
            ch_.basic_ack(delivery_tag=method.delivery_tag)
            if job["attempt"] <= MAX_ATTEMPTS:
                time.sleep(min(2 ** job["attempt"], 30))
                republish(job)
            else:
                to_dlq(job, e)
        except Exception as e:
            ch_.basic_ack(delivery_tag=method.delivery_tag)
            to_dlq(job, e)

    ch.basic_consume(queue=QUEUE, on_message_callback=on_msg)
    print("Worker listening…")
    ch.start_consuming()

if __name__ == "__main__":
    main()
