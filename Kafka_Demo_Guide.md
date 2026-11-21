# Apache Kafka Demonstration: Hands-On Guide

**Session type:** Demonstration · **Duration:** 2 hours · **Stack:** Docker, Kafka, Zookeeper, Python

This guide walks you through the whole Kafka demo from an empty folder to a working real-time pipeline. Every command is shown twice, once for **Linux / macOS (bash)** and once for **Windows (PowerShell)**, and every command has a plain-English explanation next to it. It runs on your laptop first. After that there are step-by-step instructions to run the same thing on **AWS**, and, if you want, on **GCP**.

> **How to read the command blocks.** Where the two shells differ you will see a `bash` block and a `powershell` block. Where a command is identical (most `docker` commands are) you will see one block marked **both**.

---

## Table of contents

1. [What we are building](#1-what-we-are-building)
2. [Prerequisites](#2-prerequisites)
3. [Project folder and files](#3-project-folder-and-files)
4. [The dataset](#4-the-dataset)
5. [Start Kafka with Docker](#5-start-kafka-with-docker)
6. [Core concepts in action: topics and partitions](#6-core-concepts-in-action-topics-and-partitions)
7. [Producing messages](#7-producing-messages)
8. [Consuming messages in real time](#8-consuming-messages-in-real-time)
9. [Consumer groups and offsets](#9-consumer-groups-and-offsets)
10. [Retention and fault tolerance](#10-retention-and-fault-tolerance)
11. [Running it on AWS](#11-running-it-on-aws)
12. [Optional: running it on GCP](#12-optional-running-it-on-gcp)
13. [Troubleshooting](#13-troubleshooting)
14. [Clean up](#14-clean-up)
15. [Suggested session timeline](#15-suggested-session-timeline)

---

## 1. What we are building

We will pretend to be the team behind a busy website. Every request to the site (a login, a page view, a checkout) becomes an **event**. A **producer** reads those events from a CSV file and publishes them to a Kafka **topic** called `web-logs`. A **consumer** subscribes to that topic and reacts to each event as it arrives, flagging any server errors (HTTP 5xx).

```
 web_logs_sample.csv  ──►  producer.py  ──►  [ Kafka topic: web-logs ]  ──►  consumer.py
   (60 log events)                           ├─ partition 0                  (prints events,
                                             ├─ partition 1                   flags 5xx errors)
                                             └─ partition 2
```

Why this is a good demo: the data is small enough to read on screen, but it behaves just like a real log stream. You can watch messages being spread over partitions, see two consumers share the work, and replay the whole stream by resetting an offset.

## 2. Prerequisites

| What | Why | Check it |
|---|---|---|
| Docker (Docker Desktop on Windows/macOS, Docker Engine on Linux) with Compose v2 | Runs Kafka and Zookeeper in containers, so there is nothing to install by hand | `docker --version` and `docker compose version` |
| Python 3.9 or newer | Runs the producer and consumer scripts | `python3 --version` (Linux) · `python --version` (PowerShell) |
| A terminal | Linux/macOS: any terminal. Windows: PowerShell 5.1 or 7 | n/a |
| About 4 GB of free RAM | Kafka + Zookeeper + the UI are not tiny | n/a |

**Check Docker is really running** (a common stumble on Windows is Docker Desktop being installed but not started):

**both**
```
docker --version
docker compose version
docker run --rm hello-world
```

- `docker --version` prints the installed version, which proves the CLI exists.
- `docker compose version` proves Compose v2 is available (we use `docker compose`, with a space, not the older `docker-compose`).
- `docker run --rm hello-world` pulls a tiny test image and runs it. If you see "Hello from Docker!", the engine is up. `--rm` deletes the container afterwards so it leaves no clutter.

## 3. Project folder and files

### 3.1 Create the folder

**Linux / macOS (bash)**
```bash
mkdir -p kafka-demo/data
cd kafka-demo
```

**Windows (PowerShell)**
```powershell
New-Item -ItemType Directory -Force -Path kafka-demo\data
Set-Location kafka-demo
```

- `mkdir -p` / `New-Item -Force` create the folder (and the `data` subfolder) and don't complain if they already exist.
- `cd` / `Set-Location` moves you into it. Run every later command from inside `kafka-demo`.

When you are finished adding the files below, the folder should look like this:

```
kafka-demo/
├── docker-compose.yml        # starts Zookeeper, Kafka and a web UI
├── requirements.txt          # Python dependency list
├── generate_dataset.py       # builds the sample CSV
├── producer.py               # reads the CSV and publishes to Kafka
├── consumer.py               # reads from Kafka and prints events
└── data/
    └── web_logs_sample.csv   # the dataset (created in section 4)
```

Create each file with your favourite editor (`nano docker-compose.yml`, `notepad docker-compose.yml`, or `code .` for VS Code) and paste in the contents below. If you downloaded the `kafka-demo.zip` that came with this guide, the files are already there and you can skip to section 4.

### 3.2 `docker-compose.yml`

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zk-data:/var/lib/zookeeper/data
      - zk-log:/var/lib/zookeeper/log

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://${KAFKA_HOST:-localhost}:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    volumes:
      - kafka-data:/var/lib/kafka/data

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: demo
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092

volumes:
  zk-data:
  zk-log:
  kafka-data:
```

**What each part does**

| Line / block | Meaning |
|---|---|
| `zookeeper` service | Kafka (in this version) uses Zookeeper to keep cluster metadata, such as which brokers are alive and where topics live. `ZOOKEEPER_CLIENT_PORT: 2181` is the port Kafka talks to it on. |
| `kafka` service | The actual broker, the thing that stores and serves messages. |
| `depends_on` | Start Zookeeper before Kafka. |
| `ports: "9092:9092"` | Makes the broker reachable from your host machine on port 9092, which is how `producer.py` will connect. |
| `KAFKA_ZOOKEEPER_CONNECT` | Tells the broker where Zookeeper is. `zookeeper` is the container name, and Docker resolves it for us. |
| `KAFKA_LISTENERS` / `KAFKA_ADVERTISED_LISTENERS` | The most confusing setting in Kafka, so here it is plainly. A broker tells clients "reconnect to me at *this* address". We need two addresses: **INTERNAL** (`kafka:29092`) for containers on the Docker network, and **EXTERNAL** (`localhost:9092`) for your own scripts. If the advertised address is wrong, clients connect and then hang. |
| `${KAFKA_HOST:-localhost}` | Use the environment variable `KAFKA_HOST` if set, otherwise `localhost`. On AWS/GCP we set it to the VM's public IP so remote clients can connect. |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` | Kafka stores consumer offsets in an internal topic. With one broker, its replication factor can only be 1. |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` | Topics must be created on purpose. In demos this avoids a typo silently creating a wrong topic with default settings. |
| `kafka-ui` service | A browser dashboard at port 8080, handy for showing topics, partitions and messages visually. |
| `volumes:` | Named Docker volumes keep Kafka and Zookeeper data even if the containers are removed, which we rely on in the fault-tolerance demo. |

### 3.3 `requirements.txt`

```
confluent-kafka>=2.3.0
```

One library, `confluent-kafka`, the official Python client built on `librdkafka`. It is fast and works the same on Windows, macOS and Linux.

### 3.4 `producer.py`

```python
import argparse, csv, json, time
from confluent_kafka import Producer


def on_delivery(err, msg):
    # Called once the broker confirms (or rejects) each message
    if err:
        print(f"FAILED: {err}")
    else:
        print(f"sent key={msg.key().decode()} -> partition {msg.partition()}, offset {msg.offset()}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="web-logs")
    ap.add_argument("--file", default="data/web_logs_sample.csv")
    ap.add_argument("--delay", type=float, default=0.5, help="seconds between messages")
    ap.add_argument("--limit", type=int, default=0, help="0 = send everything")
    a = ap.parse_args()

    producer = Producer({"bootstrap.servers": a.bootstrap})
    with open(a.file, newline="") as f:
        for i, row in enumerate(csv.DictReader(f), start=1):
            if a.limit and i > a.limit:
                break
            # key = user_id, so one user's events always land in the same partition
            producer.produce(a.topic, key=row["user_id"], value=json.dumps(row), callback=on_delivery)
            producer.poll(0)
            time.sleep(a.delay)
    producer.flush()
    print("done")


if __name__ == "__main__":
    main()
```

**How it works.** It opens the CSV and, for each row, calls `producer.produce(...)`. The **key** is the `user_id` and the **value** is the whole row as JSON. Because Kafka hashes the key to choose a partition, the same user always lands in the same partition, which means each user's events stay in order. `on_delivery` is a callback that fires when the broker acknowledges the message, and that is where we print the partition and offset. `flush()` at the end waits until nothing is left in the send buffer.

### 3.5 `consumer.py`

```python
import argparse, json
from confluent_kafka import Consumer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="web-logs")
    ap.add_argument("--group", default="log-readers")
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="0 = read continuously")
    a = ap.parse_args()

    consumer = Consumer({
        "bootstrap.servers": a.bootstrap,
        "group.id": a.group,
        # only used when the group has no saved offset yet
        "auto.offset.reset": "earliest" if a.from_beginning else "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([a.topic])
    print(f"listening on '{a.topic}' as group '{a.group}' (Ctrl+C to stop)")
    count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("error:", msg.error())
                continue
            ev = json.loads(msg.value())
            alert = "   <-- SERVER ERROR" if int(ev["status"]) >= 500 else ""
            print(f"[p{msg.partition()} @{msg.offset():>3}] {ev['user_id']} {ev['method']:<4} {ev['url']:<16} {ev['status']}{alert}")
            count += 1
            if a.limit and count >= a.limit:
                break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
```

**How it works.** `group.id` puts this consumer into a **consumer group**. `subscribe` says which topic to read. `poll(1.0)` asks Kafka for the next message, waiting up to one second. For each message we print the partition (`p`), the offset (`@`), and a few fields, and add a warning when the HTTP status is 500 or above. `auto.offset.reset` only matters the first time a group is seen: `earliest` starts from the oldest message, `latest` from only new ones. After the group has committed offsets, Kafka resumes from where it left off, ignoring this setting.

## 4. The dataset

The demo uses a small, made-up web server log with **60 events**, six users (`u101` to `u106`) and a realistic mix of statuses: mostly 200s, a few redirects and 404s, and a handful of 500/503 errors for the consumer to flag. It is well under 1 MB, so it loads instantly.

| Column | Example | Meaning |
|---|---|---|
| `event_id` | `1` | Unique number of the event |
| `timestamp` | `2026-11-21T10:00:07` | When the request happened |
| `user_id` | `u106` | Who made it (**we use this as the Kafka key**) |
| `ip` | `10.0.1.16` | Client IP address |
| `method` | `POST` | HTTP method |
| `url` | `/login` | Page or API requested |
| `status` | `200` | HTTP status code |
| `bytes` | `18324` | Response size |
| `response_ms` | `513` | Time taken to respond |

### 4.1 `generate_dataset.py`

```python
"""Creates data/web_logs_sample.csv - 60 fake web-server log events."""
import csv, random
from datetime import datetime, timedelta

random.seed(42)  # same file every time
users = {"u101": "10.0.1.11", "u102": "10.0.1.12", "u103": "10.0.1.13",
         "u104": "10.0.1.14", "u105": "10.0.1.15", "u106": "10.0.1.16"}
urls = ["/home", "/login", "/products", "/products/42", "/cart", "/checkout", "/api/orders", "/search?q=kafka"]
start = datetime(2026, 11, 21, 10, 0, 0)

with open("data/web_logs_sample.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["event_id", "timestamp", "user_id", "ip", "method", "url", "status", "bytes", "response_ms"])
    for i in range(1, 61):
        user = random.choice(list(users))
        url = random.choice(urls)
        method = "POST" if url in ("/login", "/checkout", "/api/orders") else "GET"
        status = random.choices([200, 301, 404, 500, 503], weights=[78, 6, 8, 5, 3])[0]
        w.writerow([i, (start + timedelta(seconds=i * 7)).isoformat(), user, users[user], method, url,
                    status, random.randint(300, 52000), random.randint(12, 1800)])
print("wrote data/web_logs_sample.csv")
```

### 4.2 Create the CSV and look at it

**Linux / macOS (bash)**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python generate_dataset.py
head -5 data/web_logs_sample.csv
wc -l data/web_logs_sample.csv
```

**Windows (PowerShell)**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python generate_dataset.py
Get-Content data\web_logs_sample.csv -TotalCount 5
(Get-Content data\web_logs_sample.csv).Count
```

- `python -m venv .venv` creates an isolated Python environment in a folder called `.venv`, so we don't pollute your system Python.
- `source .venv/bin/activate` (or `Activate.ps1`) switches your terminal to use it. You'll see `(.venv)` in the prompt.
- `pip install -r requirements.txt` installs everything listed in the requirements file.
- `python generate_dataset.py` writes `data/web_logs_sample.csv`. A fixed random seed means everybody gets the identical file.
- `head -5` / `Get-Content -TotalCount 5` shows the first five lines. `wc -l` / `.Count` counts lines. Expect **61** (60 events + 1 header).

> **PowerShell blocked the activate script?** Run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, answer `Y`, and try again. That allows locally-created scripts for your user only.

The first lines will look like this:

```
event_id,timestamp,user_id,ip,method,url,status,bytes,response_ms
1,2026-11-21T10:00:07,u106,10.0.1.16,POST,/login,200,18324,513
2,2026-11-21T10:00:14,u102,10.0.1.12,GET,/products,200,44648,1528
3,2026-11-21T10:00:21,u105,10.0.1.15,POST,/login,200,2382,73
```

> **Want a bigger or real dataset?** Any CSV under roughly 50–100 MB works. Two good Kaggle options are the *Web Server Access Logs (labeled)* dataset and the *Employee Attrition* dataset. For another CSV, change `--file` on the producer and pick a sensible column to use as the key.

## 5. Start Kafka with Docker

**both**
```
docker compose up -d
docker compose ps
```

- `docker compose up -d` reads `docker-compose.yml`, pulls the images (the first time takes a few minutes), and starts all three containers. `-d` means *detached*: it runs in the background and gives your terminal back.
- `docker compose ps` lists the containers and their state. You want `zookeeper`, `kafka` and `kafka-ui` all showing `running`.

Wait about 20 seconds, then check that Kafka has finished starting:

**Linux / macOS (bash)**
```bash
docker compose logs kafka | grep -i "started (kafka.server.KafkaServer)"
```

**Windows (PowerShell)**
```powershell
docker compose logs kafka | Select-String "started \(kafka.server.KafkaServer\)"
```

- `docker compose logs kafka` prints the broker's log. We filter it for the line saying the server has started. `grep -i` / `Select-String` is just a text search.

Open **http://localhost:8080** to see the Kafka UI. The cluster `demo` should show one broker.

**Quick health check from inside the broker:**

**both**
```
docker exec kafka kafka-broker-api-versions --bootstrap-server kafka:29092
```

- `docker exec kafka ...` runs a command inside the running `kafka` container. Here we ask the broker which API versions it supports. A long list of output means Kafka is alive and answering. `kafka:29092` is the internal address we configured earlier.

## 6. Core concepts in action: topics and partitions

A **topic** is a named stream of messages (think: a folder of log files). A **partition** is one ordered slice of that topic. More partitions mean more parallelism. Messages inside one partition keep their order. The **replication factor** is how many copies of each partition exist (we can only use 1 with one broker).

### 6.1 Create the topic

**both**
```
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic web-logs --partitions 3 --replication-factor 1
```

- `kafka-topics` is Kafka's command for managing topics.
- `--create --topic web-logs` makes a topic with that name.
- `--partitions 3` splits it into three partitions, numbered 0, 1 and 2. We chose 3 so we can later show three partitions and up to three consumers sharing the work.
- `--replication-factor 1` keeps one copy of each partition. That is the maximum on a single-broker setup.

### 6.2 List and describe it

**both**
```
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic web-logs
```

- `--list` shows all topics. You should see `web-logs`.
- `--describe` shows the details. Expect something like this:

```
Topic: web-logs   PartitionCount: 3   ReplicationFactor: 1
  Topic: web-logs  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Topic: web-logs  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Topic: web-logs  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

How to read it: **Leader** is the broker currently serving that partition (broker 1 for all, since we only have one). **Replicas** are the brokers holding a copy. **Isr** (in-sync replicas) are the copies that are fully caught up. In a healthy cluster, Replicas and Isr match.

## 7. Producing messages

### 7.1 Type a few messages by hand (console producer)

**both**
```
docker exec -it kafka kafka-console-producer --bootstrap-server kafka:29092 --topic web-logs --property parse.key=true --property key.separator=:
```

- `-it` gives the command an interactive terminal so you can type into it.
- `kafka-console-producer` sends every line you type as a message.
- `parse.key=true` and `key.separator=:` mean "treat what's before the first `:` as the key, and the rest as the value".

Now type these lines, pressing Enter after each:

```
u101:{"url":"/home","status":200}
u102:{"url":"/login","status":200}
u101:{"url":"/checkout","status":500}
```

Press `Ctrl+C` to quit. Notice that both `u101` messages share a key, so they will end up in the same partition.

> **Tip:** If piping messages or running within non-interactive scripts, use `docker exec -i kafka ...` (omit `-t`) to prevent "the input device is not a TTY" errors.

### 7.2 Send the whole dataset (Python producer)

**Linux / macOS (bash)**
```bash
source .venv/bin/activate
python producer.py --delay 0.3
```

**Windows (PowerShell)**
```powershell
.\.venv\Scripts\Activate.ps1
python producer.py --delay 0.3
```

- The first line makes sure the virtual environment is active (skip it if you already see `(.venv)`).
- `python producer.py --delay 0.3` streams all 60 rows with a 0.3-second pause between them, so the audience can follow along. Use `--limit 10` to send only 10, or `--delay 0` to send as fast as possible.

You'll see output like:

```
sent key=u106 -> partition 2, offset 0
sent key=u102 -> partition 0, offset 0
sent key=u105 -> partition 1, offset 0
sent key=u101 -> partition 1, offset 1
```

Point out two things. First, the same key always goes to the same partition. Second, **offsets count up independently inside each partition**. That is what lets Kafka guarantee order per partition.

## 8. Consuming messages in real time

### 8.1 Read from the console

**both**
```
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic web-logs --from-beginning --property print.key=true --property print.partition=true --property print.offset=true
```

- `kafka-console-consumer` prints every message it receives.
- `--from-beginning` replays everything already stored rather than waiting for new messages.
- The three `print.*` properties add the key, partition and offset to each line, which is great for showing how data is spread out. Stop with `Ctrl+C`.

> **Tip:** Pass `--max-messages 10` if you want the console consumer to exit automatically after receiving a batch of messages rather than running continuously.

### 8.2 The real-time demo (two terminals side by side)

Open **two terminal windows** and put them next to each other.

**Terminal 1: the consumer (start this first)**

Linux / macOS:
```bash
source .venv/bin/activate
python consumer.py --group log-readers --from-beginning
```

Windows PowerShell:
```powershell
.\.venv\Scripts\Activate.ps1
python consumer.py --group log-readers --from-beginning
```

**Terminal 2: the producer**

Linux / macOS:
```bash
source .venv/bin/activate
python producer.py --delay 0.5
```

Windows PowerShell:
```powershell
.\.venv\Scripts\Activate.ps1
python producer.py --delay 0.5
```

You'll see the consumer print each event moments after the producer sends it, and highlight server errors:

```
[p2 @  0] u106 POST /login          200
[p0 @  0] u102 GET  /products       200
[p1 @  7] u103 GET  /cart           500   <-- SERVER ERROR
```

Since the first run already put messages in the topic, the consumer will first replay those (because of `--from-beginning`) and then keep up with the new ones. That's the point: Kafka *stores* the stream, so consumers can start late and still catch up.

## 9. Consumer groups and offsets

A **consumer group** is a set of consumers that share the work of reading a topic. Kafka gives each partition to exactly one consumer in the group. An **offset** is a message's position in a partition. Kafka remembers each group's last committed offset, so a restarted consumer carries on where it stopped. **Lag** is how far behind the group is: latest offset minus committed offset.

### 9.1 Look at the group

**both**
```
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --list
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group log-readers
```

- `--list` shows all groups, which will include `log-readers`.
- `--describe --group log-readers` shows, per partition, the **CURRENT-OFFSET** (what the group has read), the **LOG-END-OFFSET** (what exists) and the **LAG** (the difference). If the consumer is caught up, lag is 0.

### 9.2 Watch two consumers split the work

1. Stop any running consumers.
2. Start the **same group** in two terminals (no `--from-beginning` needed now):

   Linux / macOS:
   ```bash
   source .venv/bin/activate
   python consumer.py --group split-demo --from-beginning
   ```
   Windows PowerShell:
   ```powershell
   .\.venv\Scripts\Activate.ps1
   python consumer.py --group split-demo --from-beginning
   ```

3. In a third terminal, run `python producer.py --delay 0.2`.
4. Notice each consumer prints messages from different partitions. Start a third consumer and Kafka re-balances. Start a fourth and it sits idle, because there are only 3 partitions.
5. Stop one consumer and watch Kafka hand its partitions to the survivors. That is a *rebalance*, and it's how consumer groups tolerate failures.

Check who owns what:

**both**
```
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group split-demo --members --verbose
```

### 9.3 Replay data by resetting offsets

Stop all consumers in the group first (a group must be inactive to be reset).

**Preview the change (nothing is modified):**

**both**
```
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --group log-readers --topic web-logs --reset-offsets --to-earliest --dry-run
```

**Apply it:**

**both**
```
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --group log-readers --topic web-logs --reset-offsets --to-earliest --execute
```

- `--reset-offsets --to-earliest` moves the group's bookmark back to the very first message.
- `--dry-run` only shows what would change, and `--execute` does it. Always dry-run first.
- Start `consumer.py --group log-readers` again, and all 60 events replay. This is how teams re-process data after fixing a bug.

## 10. Retention and fault tolerance

### 10.1 Messages are not deleted when read

Unlike a typical queue, Kafka keeps messages for a set **retention** period (7 days by default) no matter who has read them. Look at the topic's settings:

**both**
```
docker exec kafka kafka-configs --bootstrap-server kafka:29092 --entity-type topics --entity-name web-logs --describe --all
```

Look for `retention.ms` (default `604800000`, which is 7 days). Let's shorten it to 1 hour to show it is configurable per topic:

**both**
```
docker exec kafka kafka-configs --bootstrap-server kafka:29092 --entity-type topics --entity-name web-logs --alter --add-config retention.ms=3600000
```

- `kafka-configs` changes configuration without restarting anything.
- `--alter --add-config retention.ms=3600000` sets retention to 3,600,000 ms (1 hour). Old data is cleaned up in the background after that time.

### 10.2 Survive a broker restart

Check how many messages are in the topic, then restart the broker and check again.

**Linux / macOS (bash)**
```bash
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:29092 --topic web-logs
docker compose restart kafka
sleep 25
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:29092 --topic web-logs
```

**Windows (PowerShell)**
```powershell
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:29092 --topic web-logs
docker compose restart kafka
Start-Sleep -Seconds 25
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:29092 --topic web-logs
```

- `GetOffsetShell` prints, for each partition, the latest offset, which is effectively the number of messages stored in that partition. The three numbers add up to the total (60 after one full producer run).
- `docker compose restart kafka` stops and starts the broker, which simulates a crash.
- `sleep 25` / `Start-Sleep` gives it time to come back up.
- The numbers should be identical afterwards: messages are written to disk, not held in memory.

### 10.3 What replication would add (talking point)

With a single broker, if the machine dies the data is unavailable. A production cluster uses **3 or more brokers** and `--replication-factor 3`. Each partition then has one *leader* and two *followers*. If the leader's broker fails, a follower in the ISR is promoted automatically and clients carry on. In the `--describe` output you would see Replicas `1,2,3` and Isr shrinking to `2,3` during an outage. Combine it with `min.insync.replicas=2` and `acks=all` on the producer for strong durability. This demo can't show a failover live because there is only one broker, so explain it with the `--describe` output above.

## 11. Running it on AWS

We'll run the same Docker setup on a single **EC2** virtual machine. Everything in sections 3–10 stays identical; only *where* it runs changes.

**What you'll create:** one Ubuntu 24.04 EC2 instance (`t3.medium`, 2 vCPU / 4 GB RAM) in the Mumbai region (`ap-south-1`) with a security group that allows SSH and Kafka access only from **your** IP.

> **Cost warning.** A `t3.medium` costs a few US cents per hour. It's cheap but not free, so follow section 14 and terminate the instance when you finish.

### 11.1 Prerequisites

- An AWS account with permission to create EC2 instances.
- The **AWS CLI v2** installed and configured: `aws configure` (asks for access key, secret key, default region `ap-south-1`, output `json`). Verify with `aws sts get-caller-identity`.
- An SSH client. Linux/macOS have one. Windows 10/11 PowerShell includes `ssh` and `scp` by default (check with `ssh -V`).

Prefer clicking? Every step below can be done in the console under **EC2 → Launch instance**. The CLI is shown because it is easier to copy and repeat.

### 11.2 Find your public IP

We'll lock the firewall to this address.

**Linux / macOS (bash)**
```bash
MY_IP=$(curl -s https://checkip.amazonaws.com)
echo $MY_IP
```

**Windows (PowerShell)**
```powershell
$MY_IP = (Invoke-RestMethod https://checkip.amazonaws.com).Trim()
$MY_IP
```

It stores your public IP in a variable called `MY_IP` so we can reuse it. Keep the same terminal open, or the variable disappears.

### 11.3 Create a key pair

The key pair is how you log in to the VM securely, since there is no password.

**Linux / macOS (bash)**
```bash
aws ec2 create-key-pair --key-name kafka-demo-key --region ap-south-1 \
  --query 'KeyMaterial' --output text > kafka-demo-key.pem
chmod 400 kafka-demo-key.pem
```

**Windows (PowerShell)**
```powershell
aws ec2 create-key-pair --key-name kafka-demo-key --region ap-south-1 `
  --query 'KeyMaterial' --output text | Out-File -Encoding ascii kafka-demo-key.pem
icacls .\kafka-demo-key.pem /inheritance:r /grant:r "$($env:USERNAME):(R)"
```

- `create-key-pair` asks AWS to generate a key pair. AWS keeps the public half and prints the private half (`KeyMaterial`), which we save to a `.pem` file. **This is the only time AWS shows it.**
- `chmod 400` / `icacls` restrict the file so only you can read it. SSH refuses to use a key that other users can read.
- The backslash `\` (bash) and the backtick `` ` `` (PowerShell) are line-continuation characters. They just let one command span several lines.
- On PowerShell we use `Out-File -Encoding ascii` instead of `>` because Windows PowerShell 5.1's `>` writes UTF-16, which breaks the key file.

### 11.4 Create the security group (firewall)

**Linux / macOS (bash)**
```bash
SG_ID=$(aws ec2 create-security-group --group-name kafka-demo-sg \
  --description "Kafka demo" --region ap-south-1 --query 'GroupId' --output text)

aws ec2 authorize-security-group-ingress --group-id $SG_ID --region ap-south-1 \
  --protocol tcp --port 22 --cidr $MY_IP/32
aws ec2 authorize-security-group-ingress --group-id $SG_ID --region ap-south-1 \
  --protocol tcp --port 9092 --cidr $MY_IP/32
```

**Windows (PowerShell)**
```powershell
$SG_ID = aws ec2 create-security-group --group-name kafka-demo-sg `
  --description "Kafka demo" --region ap-south-1 --query 'GroupId' --output text

aws ec2 authorize-security-group-ingress --group-id $SG_ID --region ap-south-1 `
  --protocol tcp --port 22 --cidr "$MY_IP/32"
aws ec2 authorize-security-group-ingress --group-id $SG_ID --region ap-south-1 `
  --protocol tcp --port 9092 --cidr "$MY_IP/32"
```

- The first command creates an empty firewall and saves its ID.
- Each `authorize-security-group-ingress` opens one port. **22** is SSH (so you can log in) and **9092** is Kafka (so scripts on your laptop can reach the broker).
- `--cidr $MY_IP/32` means "only this exact IP address". **Never use `0.0.0.0/0` for Kafka**, because that exposes an unauthenticated broker to the whole internet.
- We deliberately do **not** open port 8080 for the UI. We'll reach it through an SSH tunnel instead (section 11.8).

### 11.5 Launch the instance

**Linux / macOS (bash)**
```bash
AMI_ID=$(aws ssm get-parameter --region ap-south-1 \
  --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --query 'Parameter.Value' --output text)

INSTANCE_ID=$(aws ec2 run-instances --region ap-south-1 \
  --image-id $AMI_ID --instance-type t3.medium \
  --key-name kafka-demo-key --security-group-ids $SG_ID \
  --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=30,VolumeType=gp3}' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=kafka-demo}]' \
  --query 'Instances[0].InstanceId' --output text)

aws ec2 wait instance-running --region ap-south-1 --instance-ids $INSTANCE_ID

PUBLIC_IP=$(aws ec2 describe-instances --region ap-south-1 --instance-ids $INSTANCE_ID \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo $PUBLIC_IP
```

**Windows (PowerShell)**
```powershell
$AMI_ID = aws ssm get-parameter --region ap-south-1 `
  --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id `
  --query 'Parameter.Value' --output text

$INSTANCE_ID = aws ec2 run-instances --region ap-south-1 `
  --image-id $AMI_ID --instance-type t3.medium `
  --key-name kafka-demo-key --security-group-ids $SG_ID `
  --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=30,VolumeType=gp3}" `
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=kafka-demo}]" `
  --query 'Instances[0].InstanceId' --output text

aws ec2 wait instance-running --region ap-south-1 --instance-ids $INSTANCE_ID

$PUBLIC_IP = aws ec2 describe-instances --region ap-south-1 --instance-ids $INSTANCE_ID `
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text
$PUBLIC_IP
```

- The `ssm get-parameter` call asks AWS for the **current** official Ubuntu 24.04 image ID, so you never have to hunt for an AMI ID that differs per region.
- `run-instances` launches the VM: `t3.medium` size, your key pair, your firewall, and a 30 GB disk (the default 8 GB is too small for Docker images).
- `wait instance-running` blocks until the VM is up.
- The last command reads the VM's public IP, which you'll use for everything that follows.

### 11.6 Connect and install Docker

**Linux / macOS (bash)**
```bash
ssh -i kafka-demo-key.pem ubuntu@$PUBLIC_IP
```

**Windows (PowerShell)**
```powershell
ssh -i .\kafka-demo-key.pem ubuntu@$PUBLIC_IP
```

Type `yes` when asked about the host fingerprint. If it says "connection refused", wait 30 seconds, because the VM is still booting. You are now **on the server**, so the next commands are the same everywhere:

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
sudo apt-get install -y python3-venv unzip
exit
```

- The first line downloads and runs Docker's official install script, which installs Docker Engine **and** the Compose plugin.
- `usermod -aG docker $USER` lets the `ubuntu` user run `docker` without `sudo`.
- `apt-get install` adds Python's venv module (not included on a fresh Ubuntu).
- `exit` logs you out because the group change only takes effect on a **new** login. Reconnect with the same `ssh` command.

### 11.7 Copy the project and start Kafka

Back on **your laptop**, in the folder containing `kafka-demo`:

**Linux / macOS (bash)**
```bash
scp -i kafka-demo-key.pem -r kafka-demo ubuntu@$PUBLIC_IP:~/
```

**Windows (PowerShell)**
```powershell
scp -i .\kafka-demo-key.pem -r .\kafka-demo ubuntu@${PUBLIC_IP}:~/
```

`scp` is "secure copy", and `-r` copies the folder recursively. Now log in again and start everything. This part runs **on the VM**, so it's bash for everyone:

```bash
cd ~/kafka-demo
echo "KAFKA_HOST=$(curl -s https://checkip.amazonaws.com)" > .env
docker compose up -d
docker compose ps
```

- `.env` is read automatically by Docker Compose. We set `KAFKA_HOST` to the VM's public IP so the broker advertises an address your laptop can actually reach (remember the `${KAFKA_HOST:-localhost}` line in the compose file).
- `docker compose up -d` starts everything exactly as on your laptop.

From here, follow **sections 6 to 10 unchanged**. Run the `docker exec` commands on the VM, and create the topic first.

> **Important:** if you stop and start the instance, AWS assigns a **new** public IP. Update `.env` and run `docker compose up -d --force-recreate kafka`. To avoid this, attach an Elastic IP.

### 11.8 Reach Kafka and the UI from your laptop

**Run the producer from your laptop** against the cloud broker:

**Linux / macOS (bash)**
```bash
python producer.py --bootstrap $PUBLIC_IP:9092 --delay 0.3
```

**Windows (PowerShell)**
```powershell
python producer.py --bootstrap "${PUBLIC_IP}:9092" --delay 0.3
```

This works because the security group lets your IP reach port 9092 and the broker advertises its public IP.

**Open the Kafka UI securely** with an SSH tunnel, which forwards your local port 8080 to the VM's port 8080 through the encrypted SSH connection:

**Linux / macOS (bash)**
```bash
ssh -i kafka-demo-key.pem -L 8080:localhost:8080 ubuntu@$PUBLIC_IP
```

**Windows (PowerShell)**
```powershell
ssh -i .\kafka-demo-key.pem -L 8080:localhost:8080 ubuntu@$PUBLIC_IP
```

Leave that window open and browse to **http://localhost:8080**.

### 11.9 Tear down AWS resources (don't skip!)

**Linux / macOS (bash)**
```bash
aws ec2 terminate-instances --region ap-south-1 --instance-ids $INSTANCE_ID
aws ec2 wait instance-terminated --region ap-south-1 --instance-ids $INSTANCE_ID
aws ec2 delete-security-group --region ap-south-1 --group-id $SG_ID
aws ec2 delete-key-pair --region ap-south-1 --key-name kafka-demo-key
rm kafka-demo-key.pem
```

**Windows (PowerShell)**
```powershell
aws ec2 terminate-instances --region ap-south-1 --instance-ids $INSTANCE_ID
aws ec2 wait instance-terminated --region ap-south-1 --instance-ids $INSTANCE_ID
aws ec2 delete-security-group --region ap-south-1 --group-id $SG_ID
aws ec2 delete-key-pair --region ap-south-1 --key-name kafka-demo-key
Remove-Item .\kafka-demo-key.pem
```

Termination deletes the VM and its disk, so billing stops. The security group can only be deleted once the instance is gone, which is why we wait first.

> **Good to know:** AWS also offers a managed Kafka service, **Amazon MSK**. It removes the work of running brokers yourself, but it is slower to create and costs more, so a single EC2 box is the better choice for a short demonstration.

## 12. Optional: running it on GCP

The same idea on **Google Compute Engine**. We use an `e2-medium` VM (2 vCPU / 4 GB) running Ubuntu 24.04 in Mumbai (`asia-south1`). Sections 6 to 10 apply unchanged once Docker is running.

### 12.1 Prerequisites

- A Google Cloud project with billing enabled.
- The **gcloud CLI** installed. Then run:

**both**
```
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud services enable compute.googleapis.com
```

- `auth login` opens a browser to sign in. `config set project` tells gcloud which project to use (replace `YOUR_PROJECT_ID`). `services enable` switches on the Compute Engine API, which is a one-time step per project.

### 12.2 Find your IP

**Linux / macOS (bash)**
```bash
MY_IP=$(curl -s https://checkip.amazonaws.com)
```

**Windows (PowerShell)**
```powershell
$MY_IP = (Invoke-RestMethod https://checkip.amazonaws.com).Trim()
```

### 12.3 Create the firewall rule

**Linux / macOS (bash)**
```bash
gcloud compute firewall-rules create allow-kafka-demo \
  --direction=INGRESS --allow=tcp:9092 \
  --source-ranges=$MY_IP/32 --target-tags=kafka-demo
```

**Windows (PowerShell)**
```powershell
gcloud compute firewall-rules create allow-kafka-demo `
  --direction=INGRESS --allow=tcp:9092 `
  --source-ranges="$MY_IP/32" --target-tags=kafka-demo
```

This opens port 9092 **only** to your IP, and **only** for VMs tagged `kafka-demo`. We don't need to open SSH because `gcloud compute ssh` handles it through the default network rule.

### 12.4 Create the VM

**Linux / macOS (bash)**
```bash
gcloud compute instances create kafka-demo \
  --zone=asia-south1-a --machine-type=e2-medium \
  --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
  --boot-disk-size=30GB --tags=kafka-demo
```

**Windows (PowerShell)**
```powershell
gcloud compute instances create kafka-demo `
  --zone=asia-south1-a --machine-type=e2-medium `
  --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud `
  --boot-disk-size=30GB --tags=kafka-demo
```

- `--image-family` picks the latest Ubuntu 24.04 image. `--tags=kafka-demo` attaches the firewall rule from the previous step. The command prints the VM's **EXTERNAL_IP**.

Get the IP any time with:

**Linux / macOS (bash)**
```bash
PUBLIC_IP=$(gcloud compute instances describe kafka-demo --zone=asia-south1-a \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)')
echo $PUBLIC_IP
```

**Windows (PowerShell)**
```powershell
$PUBLIC_IP = gcloud compute instances describe kafka-demo --zone=asia-south1-a `
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
$PUBLIC_IP
```

### 12.5 Copy files, install Docker, start Kafka

**Linux / macOS (bash)**
```bash
gcloud compute scp --recurse kafka-demo kafka-demo:~ --zone=asia-south1-a
gcloud compute ssh kafka-demo --zone=asia-south1-a
```

**Windows (PowerShell)**
```powershell
gcloud compute scp --recurse .\kafka-demo kafka-demo:~ --zone=asia-south1-a
gcloud compute ssh kafka-demo --zone=asia-south1-a
```

`gcloud compute scp` copies the folder, and `gcloud compute ssh` logs you in. The first time, it creates an SSH key for you automatically (press Enter for no passphrase). Once on the VM, the commands are the same for everyone:

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
sudo apt-get install -y python3-venv
exit
```

Reconnect with `gcloud compute ssh kafka-demo --zone=asia-south1-a`, then:

```bash
cd ~/kafka-demo
echo "KAFKA_HOST=$(curl -s -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip)" > .env
docker compose up -d
docker compose ps
```

The `.env` line asks GCP's internal metadata server for the VM's external IP, which is the GCP equivalent of the `checkip` call we used on AWS. Continue with sections 6 to 10.

### 12.6 UI access and tear down

Tunnel the UI to your laptop:

**both**
```
gcloud compute ssh kafka-demo --zone=asia-south1-a -- -L 8080:localhost:8080
```

Then open http://localhost:8080. When you've finished, delete everything:

**both**
```
gcloud compute instances delete kafka-demo --zone=asia-south1-a --quiet
gcloud compute firewall-rules delete allow-kafka-demo --quiet
```

## 13. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker isn't running | Start Docker Desktop (Windows/macOS) or `sudo systemctl start docker` (Linux) |
| `docker compose` not recognised | Old Docker, or the old `docker-compose` binary | Update Docker, and use `docker compose` with a space |
| Port 9092 or 8080 already in use | Another app is using it | Stop that app, or change the left side of the port mapping (e.g. `"9093:9092"`) |
| Producer hangs or says `Broker transport failure` | Advertised listener is wrong | On a laptop use `localhost:9092`. On a VM make sure `.env` has the right public IP and recreate Kafka |
| `Unknown topic or partition` | Topic doesn't exist (auto-create is off) | Run the create command from section 6.1 |
| Consumer prints nothing | Group already read everything, and you didn't pass `--from-beginning` | Use a new `--group`, or reset offsets (section 9.3) |
| `Group ... is not Empty` when resetting offsets | Consumers still running | Stop all consumers in that group first |
| Kafka container keeps restarting after `down -v` then `up` | Stale Zookeeper state | `docker compose down -v` (removes volumes) then `up -d` for a clean start |
| `Activate.ps1 cannot be loaded` | PowerShell execution policy | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| `Permission denied (publickey)` on SSH | Wrong key, wrong user, or loose key permissions | Use `ubuntu@` on both clouds' Ubuntu image, and re-run `chmod 400` / `icacls` |
| SSH works but port 9092 times out from laptop | Your IP changed | Re-run the `authorize-security-group-ingress` (AWS) or update the firewall rule (GCP) with the new IP |

## 14. Clean up

**Local machine: stop the containers but keep the data:**

**both**
```
docker compose down
```

**Local machine: stop and wipe everything, including stored messages:**

**both**
```
docker compose down -v
```

- `down` stops and removes the containers. Named volumes survive, so the messages are still there next time.
- `down -v` also deletes the volumes. Use it for a perfectly fresh start.

Then deactivate the Python environment by typing `deactivate`. Remember to also tear down any cloud resources (sections 11.9 and 12.6).

## 15. Suggested session timeline

| Time | Section | What to do |
|---|---|---|
| 0:00–0:15 | Setup and context | Explain why streaming matters. Run `docker compose up -d`, show `docker compose ps` and the UI |
| 0:15–0:55 | Core concepts | Sections 6 and 9.1: topic, partitions, `--describe`, producers, consumers, groups, offsets |
| 0:55–1:05 | Break | Mandatory 10-minute break |
| 1:05–2:00 | Streaming demo | Sections 7, 8, 9.2, 9.3 and 10: live producer/consumer, rebalancing, lag, replay, retention, restart |
| 2:00–2:10 | Wrap-up | Recap, then the quiz slides |

**Tip for the instructor:** pre-pull the images (`docker compose pull`) and run a full dry run before the session. Keep an AI assistant open to explain error messages live, since that makes a nice example of using an LLM for troubleshooting.
