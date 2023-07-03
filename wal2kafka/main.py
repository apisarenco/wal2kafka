from time import sleep
from typing import Any, Protocol
import psycopg
from os import getenv
from pathlib import Path
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from subprocess import Popen, PIPE
from threading import Thread, Event
from queue import Empty, Queue
import msgspec
import signal
from loguru import logger


pg_host = getenv("POSTGRES_HOST", "postgresql")
pg_port = getenv("POSTGRES_PORT", "5432")
pg_db = getenv("POSTGRES_DB", "postgres")
pg_user = getenv("POSTGRES_USER", "postgres")
pg_password = getenv("POSTGRES_PASSWORD", "postgres")
replica_slot = getenv("REPLICA_SLOT", "test_slot")
kafka_servers = getenv("KAFKA_SERVERS", "localhost:9092").split(",")
kafka_topic = getenv("KAFKA_TOPIC", "wal2kafka")
kafka_rejects_topic = getenv("KAFKA_REJECTS_TOPIC", "wal2kafka_rejects")
kafka_key = getenv("KAFKA_KEY", "wal2kafka").encode("utf-8")


def connect():
    return psycopg.connect(f"host={pg_host} port={pg_port} dbname={pg_db} user={pg_user} password={pg_password}")

with connect() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT count(1) FROM pg_replication_slots WHERE slot_name = %s", (replica_slot,))
        num_rows = cur.fetchall()
    if not num_rows or num_rows[0] == 0:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM pg_create_logical_replication_slot(%s, 'wal2json')", (replica_slot,))
            cur.fetchone()


pgpassfile = Path("/tmp/.pgpass")
with pgpassfile.open("w", encoding="utf-8") as f:
    f.write(f"{pg_host}:{pg_port}:{pg_db}:{pg_user}:{pg_password}")
pgpassfile.chmod(0o600)


admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_servers, 
    client_id='wak2kafka'
)
topics_to_ensure = [kafka_topic, kafka_rejects_topic]
existing_topics = set(admin_client.list_topics())
topic_list = []
for topic in topics_to_ensure:
    if topic not in existing_topics:
        logger.info("Creating topic '{topic}'", topic=topic)
        topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
if topic_list:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)


class Future(Protocol):
    def get(self, timeout: float | None = None) -> Any:
        ...


class CDCColumn(msgspec.Struct, omit_defaults=True, frozen=True):
    name: str
    type: str
    value: Any


class CDCEvent(msgspec.Struct, omit_defaults=True, frozen=True):
    action: str
    schema: str = ""
    table: str = ""
    columns: list[CDCColumn] = msgspec.field(default_factory=list)
    identity: list[CDCColumn] = msgspec.field(default_factory=list)


class Receiver(Thread):
    def __init__(self, output_queue: Queue[CDCEvent | bytes], exit_event: Event):
        super().__init__()
        self.output_queue = output_queue
        self.exit_event = exit_event
        self.process: Popen | None = None
        self.decoder = msgspec.json.Decoder(type=CDCEvent)

    def run(self):
        self.process = Popen(
            [
                "/usr/bin/pg_recvlogical",
                "-h", "postgresql",
                "-p", "5432",
                "-U", "postgres",
                "-d", "postgres",
                "--slot", "test_slot",
                "--start",
                "-o", "format-version=2",
                "-f", "-"
            ],
            stdout=PIPE,
            stderr=PIPE,
            env={"PGPASSFILE": str(pgpassfile)},
        )
        while not self.exit_event.is_set() and self.process.stdout is not None:
            data = self.process.stdout.readline()
            if not data:
                continue
            try:
                event = self.decoder.decode(data)
            except Exception as e:
                logger.error("Failed to decode event '{event}' due to error: {e}", event=data.decode("utf-8"), e=e)
                self.output_queue.put(data)
            else:
                self.output_queue.put(event)
        if self.exit_event.is_set():
            logger.info("Stopping pg_recvlogical")
            self.process.send_signal(signal.SIGINT)
            for i in range(10):
                if self.process.wait(5) is not None:
                    logger.info("pg_recvlogical stopped")
                    return
                logger.info("pg_recvlogical did not stop yet")
            logger.error("pg_recvlogical did not stop after 10 seconds, killing it")
            self.process.terminate()


class Sender(Thread):
    def __init__(self, input_queue: Queue[CDCEvent | bytes], futures: Queue[Future], exit_event: Event):
        super().__init__()
        self.input_queue = input_queue
        self.futures = futures
        self.exit_event = exit_event
        self.encoder = msgspec.json.Encoder()

    def connect(self):
        return KafkaProducer(bootstrap_servers=kafka_servers)

    def run(self):
        producer: KafkaProducer | None = None
        while not self.exit_event.is_set() or not self.input_queue.empty():
            try:
                event = self.input_queue.get(timeout=0.1)
            except Empty:
                continue
            while True:
                try:
                    if producer is None:
                        producer = self.connect()
                    if isinstance(event, bytes):
                        future = producer.send("rejects", event, key=kafka_key)
                    else:
                        future = producer.send(kafka_topic, self.encoder.encode(event), key=kafka_key)
                    self.futures.put(future)
                    break
                except Exception as e:
                    logger.error("Failed to send event '{event}' due to error: {e}", event=event, e=e)
                    if producer is not None:
                        producer.close()
                    producer = None
                    sleep(5)


class Committer(Thread):
    def __init__(self, futures: Queue[Future], exit_event: Event):
        super().__init__()
        self.futures = futures
        self.exit_event = exit_event

    def run(self):
        while not self.exit_event.is_set() or not self.futures.empty():
            try:
                future = self.futures.get(timeout=0.1)
                future.get()
                logger.info("Event committed")
            except Empty:
                continue


event_queue = Queue[CDCEvent | bytes](maxsize=1)
futures_queue = Queue[Future](maxsize=10)
exit_event = Event()

signal.signal(signal.SIGINT, lambda signum, frame: exit_event.set())
signal.signal(signal.SIGTERM, lambda signum, frame: exit_event.set())

receiver = Receiver(event_queue, exit_event)
sender = Sender(event_queue, futures_queue, exit_event)
committer = Committer(futures_queue, exit_event)

workers = [receiver, sender, committer]
for worker in workers:
    worker.start()

for worker in workers:
    worker.join()
