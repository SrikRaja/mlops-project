
"""
Kafka Consumer & Metrics Harness - DA5402W Assignment 1, Part A

Covers tasks 1-7 of Part A:
  1-2. Create topic with 3 partitions, replication factor 1
  3. Execute the provided producer and verify records are published
  4. Consume records and verify correct delivery
  5. Collect metrics: total produced/consumed, per-partition counts, throughput
  6. Demonstrate varying consumer counts (1, 2, 4) via manual partition assignment
  7. Demonstrate two independent consumer groups

Run:
    python -m assignment1.kafka --rollno da25m624
"""

import argparse
import ast
import json
import os
import subprocess
import sys
import threading
import time
from collections import defaultdict

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

PARTITIONS = 3
REPLICATION_FACTOR = 1


def create_topic(bootstrap_servers, topic, partitions=PARTITIONS, replication_factor=REPLICATION_FACTOR):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    existing = admin.list_topics()
    if topic in existing:
        print(
            f"Topic '{topic}' already exists — deleting to recreate with {partitions} partitions...")
        admin.delete_topics([topic])
        time.sleep(3)
    admin.create_topics([NewTopic(
        name=topic, num_partitions=partitions, replication_factor=replication_factor)])
    print(
        f"Created topic '{topic}' with {partitions} partitions, replication factor {replication_factor}")
    admin.close()
    time.sleep(2)


def get_topic_config(bootstrap_servers, topic):
    """Fetch and return topic configuration details for the report."""
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    metadata = admin.describe_topics([topic])
    admin.close()

    topic_meta = metadata[0]
    partitions_info = []
    for p in topic_meta["partitions"]:
        partitions_info.append({
            "partition": p["partition"],
            "leader": p["leader"],
            "replicas": p["replicas"],
            "isr": p["isr"],
        })

    config = {
        "topic": topic,
        "bootstrap_servers": bootstrap_servers,
        "num_partitions": len(topic_meta["partitions"]),
        "replication_factor": len(topic_meta["partitions"][0]["replicas"]) if topic_meta["partitions"] else None,
        "partitions_detail": partitions_info,
    }
    return config


def run_producer(topic, records, rate, bootstrap_servers):
    cmd = [
        sys.executable, "-m", "assignment1.producer",
        "--topic", topic,
        "--records", str(records),
        "--rate", str(rate),
        "--bootstrap-servers", bootstrap_servers,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print("Producer STDERR:", result.stderr)

    metrics = None
    for line in reversed(result.stdout.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                metrics = ast.literal_eval(line)
            except Exception:
                metrics = None
            break
    return metrics


def consume_fixed(topic, group_id, consumer_id, bootstrap_servers, timeout_ms, results):
    """Group-based consumption (used for baseline verification and Task 7 group independence)."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout_ms,
    )
    count = 0
    partition_counts = defaultdict(int)
    start = time.time()
    for msg in consumer:
        count += 1
        partition_counts[msg.partition] += 1
    elapsed = time.time() - start
    consumer.close()
    results[consumer_id] = {"count": count, "partitions": dict(
        partition_counts), "elapsed_sec": elapsed}


def consume_assigned(topic, partitions, consumer_id, bootstrap_servers, timeout_ms, results):
    """
    Manual partition-assignment consumption (used for Task 6 consumer scaling).
    Deterministically assigns a fixed set of partitions to this consumer,
    avoiding the group-rebalance race condition that occurs when data is
    already fully published before consumers join.
    """
    if not partitions:
        # More consumers than partitions -> this consumer gets nothing, sits idle.
        results[consumer_id] = {"count": 0, "partitions": {
        }, "elapsed_sec": 0.0, "assigned_partitions": []}
        return

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout_ms,
    )
    tps = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(tps)
    consumer.seek_to_beginning(*tps)

    count = 0
    partition_counts = defaultdict(int)
    start = time.time()
    for msg in consumer:
        count += 1
        partition_counts[msg.partition] += 1
    elapsed = time.time() - start
    consumer.close()
    results[consumer_id] = {
        "count": count,
        "partitions": dict(partition_counts),
        "elapsed_sec": elapsed,
        "assigned_partitions": partitions,
    }


def baseline_run(topic, bootstrap_servers, records, rate):
    print("\n=== Baseline: single producer -> single consumer ===")
    create_topic(bootstrap_servers, topic, PARTITIONS, REPLICATION_FACTOR)
    producer_metrics = run_producer(topic, records, rate, bootstrap_servers)
    results = {}
    consume_fixed(topic, "baseline_group", 0,
                  bootstrap_servers, 20000, results)
    r = results[0]
    consumer_throughput = r["count"] / \
        r["elapsed_sec"] if r["elapsed_sec"] > 0 else 0

    report = {
        "records_produced": producer_metrics["records_published"] if producer_metrics else records,
        "producer_throughput_rps": producer_metrics["producer_throughput_rps"] if producer_metrics else None,
        "records_consumed": r["count"],
        "consumer_throughput_rps": consumer_throughput,
        "per_partition_counts": r["partitions"],
    }
    print("Baseline report:", json.dumps(report, indent=2))
    return report


def demonstrate_consumer_scaling(topic, bootstrap_servers, records, rate):
    print("\n=== Task 6: Varying consumer count (1, 2, 4) via manual partition assignment ===")
    scaling_report = {}
    for n in [1, 2, 4]:
        print(f"\n--- {n} consumer(s), {PARTITIONS} partitions ---")
        create_topic(bootstrap_servers, topic, PARTITIONS, REPLICATION_FACTOR)
        producer_metrics = run_producer(
            topic, records, rate, bootstrap_servers)

        # Round-robin distribute the fixed partitions [0, 1, 2] across n consumers.
        # If n > PARTITIONS, the extra consumer(s) get an empty list -> idle.
        assignment = {i: [] for i in range(n)}
        for idx, p in enumerate(range(PARTITIONS)):
            assignment[idx % n].append(p)

        print(f"Partition assignment: {assignment}")

        results = {}
        threads = [
            threading.Thread(target=consume_assigned, args=(
                topic, assignment[i], i, bootstrap_servers, 15000, results))
            for i in range(n)
        ]
        start = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.time() - start

        total_consumed = sum(r["count"] for r in results.values())
        throughput = total_consumed / elapsed if elapsed > 0 else 0

        scaling_report[n] = {
            "producer_metrics": producer_metrics,
            "partition_assignment": assignment,
            "per_consumer": results,
            "total_consumed": total_consumed,
            "elapsed_sec": elapsed,
            "consumer_throughput_rps": throughput,
        }
        print(
            f"Total consumed: {total_consumed}  |  throughput: {throughput:.2f} rec/s")
        for i, r in results.items():
            print(
                f"  consumer {i}: assigned={assignment[i]}  consumed={r['count']} records, partitions={r['partitions']}")
    return scaling_report


def demonstrate_consumer_groups(topic, bootstrap_servers, records, rate):
    print("\n=== Task 7: Two independent consumer groups ===")
    create_topic(bootstrap_servers, topic, PARTITIONS, REPLICATION_FACTOR)
    run_producer(topic, records, rate, bootstrap_servers)
    results = {}
    threads = [
        threading.Thread(target=consume_fixed, args=(
            topic, g, g, bootstrap_servers, 15000, results))
        for g in ["group_alpha", "group_beta"]
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for g, r in results.items():
        print(
            f"  {g}: consumed {r['count']} records (partitions={r['partitions']})")
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Kafka consumer harness - Assignment 1 Part A")
    parser.add_argument("--rollno", required=True, help="e.g. da25m624")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--records", type=int, default=3000,
                        help="records per producer run")
    parser.add_argument("--rate", type=float, default=100.0,
                        help="producer records/sec")
    args = parser.parse_args()

    topic = f"sensor_{args.rollno}"
    os.makedirs("reports", exist_ok=True)

    # --- Configuration details (for report Task 1-2) ---
    create_topic(args.bootstrap_servers, topic, PARTITIONS, REPLICATION_FACTOR)
    config = get_topic_config(args.bootstrap_servers, topic)
    print("\n=== Configuration Details ===")
    print(json.dumps(config, indent=2))
    with open("reports/kafka_config.json", "w") as f:
        json.dump(config, f, indent=2)

    # --- Task 3-5: baseline single producer -> single consumer ---
    baseline = baseline_run(
        topic, args.bootstrap_servers, args.records, args.rate)
    with open("reports/kafka_baseline_metrics.json", "w") as f:
        json.dump(baseline, f, indent=2)

    # --- Task 6: consumer scaling ---
    scaling = demonstrate_consumer_scaling(
        topic, args.bootstrap_servers, args.records, args.rate)
    with open("reports/kafka_scaling_metrics.json", "w") as f:
        json.dump(scaling, f, indent=2)

    # --- Task 7: two independent consumer groups ---
    groups = demonstrate_consumer_groups(
        topic, args.bootstrap_servers, args.records, args.rate)
    with open("reports/kafka_groups_metrics.json", "w") as f:
        json.dump(groups, f, indent=2)

    print("\nAll done. Metrics written to reports/kafka_*.json")


if __name__ == "__main__":
    main()
