"""
Spark Structured Streaming Pipeline - DA5402W Assignment 1, Part B

Reads the sensor stream from Kafka topic sensor_<rollno> and implements:
  - Data preprocessing (schema, missing-value imputation, dedup, invalid
    record removal, timestamp conversion, feature engineering)
  - Streaming analytics (avg/max temp per sensor, active sensors, status dist.)
  - Event-time processing (5-min tumbling window, watermarking, late events)

ARCHITECTURE NOTE (for report):
Two streaming queries run concurrently against the same Kafka source:

  Query A ("cleaning_query") - implemented via foreachBatch with
    driver-managed Python state (a per-sensor rolling history, a seen-keys
    set for dedup, and running analytics accumulators). This is appropriate
    for a single-node/local Spark deployment (as used here in a Codespace)
    and gives full transparency/debuggability over the exact 5-minute
    "average of the same sensor over the previous window" computation
    required by the assignment. Covers Tasks 1-11.

  Query B ("windowed_query") - a genuine Spark-native stateful streaming
    query using withWatermark() + groupBy(window(...)) + 
    dropDuplicatesWithinWatermark(). This exposes Spark's OFFICIAL
    numRowsDroppedByWatermark and stateOperators metrics directly from
    the streaming query's progress, used for Task 14 (late event handling)
    and the Performance Analysis "state store size" requirement.
    Covers Tasks 12-14.

Run:
    python -m assignment1.spark_streaming --rollno da25m624 --run-seconds 240 --feed
"""

import argparse
import json
import os
import threading
import time
from collections import defaultdict, deque, Counter
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    col, from_json, expr, window, avg as spark_avg, count as spark_count,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)
from pyspark.sql.streaming import StreamingQueryListener

# ----------------------------------------------------------------------
# Schema (Task 1)
# ----------------------------------------------------------------------
SENSOR_SCHEMA = StructType([
    StructField("sensor_id", StringType(), True),
    # raw string as sent by producer
    StructField("timestamp", StringType(), True),
    # nullable -> missing values
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("status", StringType(), True),
])

HISTORY_WINDOW_SECONDS = 5 * 60  # 5-minute rolling window for imputation
LATE_ACCEPT_THRESHOLD_SECONDS = 5 * 60  # matches watermark delay threshold

# ----------------------------------------------------------------------
# Driver-managed state for Query A
# ----------------------------------------------------------------------


class PipelineState:
    def __init__(self):
        self.lock = threading.Lock()
        # sensor_id -> deque[(event_time, temp)]
        self.sensor_history = defaultdict(deque)
        self.seen_keys = set()                     # (sensor_id, raw_timestamp_str)
        self.max_event_time_seen = None

        # counters for the summary table
        self.missing_corrected = 0
        self.duplicates_removed = 0
        self.invalid_removed = 0
        self.dropped_insufficient_history = 0
        self.late_accepted = 0
        self.records_cleaned_total = 0
        self.records_raw_total = 0

        # analytics
        self.sensor_stats = defaultdict(
            lambda: {"sum": 0.0, "count": 0, "max": float("-inf")})
        self.status_counter = Counter()

        # batch-level log for the report
        self.batch_log = []

    def summary_table(self):
        return {
            "Missing Values Corrected": self.missing_corrected,
            "Duplicate Records Removed": self.duplicates_removed,
            "Invalid Records Removed": self.invalid_removed,
            "Dropped (Insufficient History for Imputation)": self.dropped_insufficient_history,
            "Late Records Accepted": self.late_accepted,
            # "Records Discarded by Watermarking" is filled in from Query B's progress
        }

    def analytics_snapshot(self):
        now_ref = self.max_event_time_seen
        active_sensors = []
        if now_ref is not None:
            for sid, hist in self.sensor_history.items():
                if hist and (now_ref - hist[-1][0]).total_seconds() <= HISTORY_WINDOW_SECONDS:
                    active_sensors.append(sid)

        per_sensor = {}
        for sid, s in self.sensor_stats.items():
            if s["count"] > 0:
                per_sensor[sid] = {
                    "avg_temperature": round(s["sum"] / s["count"], 3),
                    "max_temperature": round(s["max"], 3),
                    "readings": s["count"],
                }

        return {
            "per_sensor_avg_max_temperature": per_sensor,
            "active_sensor_count": len(active_sensors),
            "active_sensors": sorted(active_sensors),
            "status_distribution": dict(self.status_counter),
        }


STATE = PipelineState()


def prune_history(hist, ref_time):
    while hist and (ref_time - hist[0][0]).total_seconds() > HISTORY_WINDOW_SECONDS:
        hist.popleft()


# ----------------------------------------------------------------------
# Query A: foreachBatch cleaning pipeline (Tasks 3-11)
# ----------------------------------------------------------------------
def clean_batch(batch_df, batch_id):
    batch_start = time.time()
    raw_rows = batch_df.collect()
    STATE.records_raw_total += len(raw_rows)

    cleaned_rows = []

    with STATE.lock:
        # process in event-time order so late/duplicate/history logic behaves sensibly
        def sort_key(r):
            return r["event_time"] if r["event_time"] is not None else datetime.min
        raw_rows.sort(key=sort_key)

        for r in raw_rows:
            sensor_id = r["sensor_id"]
            raw_ts = r["timestamp"]
            event_time = r["event_time"]
            temperature = r["temperature"]
            humidity = r["humidity"]
            status = r["status"]

            # --- Task 4: duplicate removal (sensor_id + timestamp) ---
            dup_key = (sensor_id, raw_ts)
            if dup_key in STATE.seen_keys:
                STATE.duplicates_removed += 1
                continue
            STATE.seen_keys.add(dup_key)

            # --- Task 5 (part): invalid/unparseable timestamp ---
            if event_time is None:
                STATE.invalid_removed += 1
                continue

            # --- Task 3: missing value imputation using previous 5-min window ---
            corrected = False
            if temperature is None:
                hist = STATE.sensor_history[sensor_id]
                prune_history(hist, event_time)
                recent_temps = [t for (_, t) in hist]
                if recent_temps:
                    temperature = sum(recent_temps) / len(recent_temps)
                    STATE.missing_corrected += 1
                    corrected = True
                else:
                    STATE.dropped_insufficient_history += 1
                    continue

            # --- Task 5: invalid temperature range ---
            if temperature < -20 or temperature > 100:
                STATE.invalid_removed += 1
                continue

            # --- late-arrival classification (informational; Task 14 in spirit) ---
            if STATE.max_event_time_seen is not None and event_time < STATE.max_event_time_seen:
                delay = (STATE.max_event_time_seen -
                         event_time).total_seconds()
                if delay <= LATE_ACCEPT_THRESHOLD_SECONDS:
                    STATE.late_accepted += 1
            else:
                STATE.max_event_time_seen = event_time

            # update rolling history (only with genuine, in-range readings)
            hist = STATE.sensor_history[sensor_id]
            hist.append((event_time, temperature))
            prune_history(hist, STATE.max_event_time_seen or event_time)

            # --- Task 7: feature engineering ---
            hour_of_day = event_time.hour
            day_of_week = event_time.weekday()          # Monday=0 .. Sunday=6
            is_weekend = 1 if day_of_week >= 5 else 0

            # --- analytics accumulation (Tasks 8-11) ---
            s = STATE.sensor_stats[sensor_id]
            s["sum"] += temperature
            s["count"] += 1
            s["max"] = max(s["max"], temperature)
            STATE.status_counter[status] += 1

            cleaned_rows.append(Row(
                sensor_id=sensor_id,
                event_time=event_time,
                temperature=float(temperature),
                humidity=humidity,
                status=status,
                hour_of_day=hour_of_day,
                day_of_week=day_of_week,
                is_weekend=is_weekend,
                temp_corrected=corrected,
            ))

        STATE.records_cleaned_total += len(cleaned_rows)

    # write cleaned micro-batch to parquet (append) if non-empty
    if cleaned_rows:
        out_schema = StructType([
            StructField("sensor_id", StringType()),
            StructField("event_time", TimestampType()),
            StructField("temperature", DoubleType()),
            StructField("humidity", DoubleType()),
            StructField("status", StringType()),
            StructField("hour_of_day", DoubleType()),
            StructField("day_of_week", DoubleType()),
            StructField("is_weekend", DoubleType()),
            StructField("temp_corrected", StringType()),
        ])
        cleaned_df = batch_df.sparkSession.createDataFrame(
            [(r.sensor_id, r.event_time, r.temperature, r.humidity, r.status,
              float(r.hour_of_day), float(r.day_of_week), float(r.is_weekend), str(r.temp_corrected))
             for r in cleaned_rows],
            schema=out_schema,
        )
        cleaned_df.write.mode("append").parquet("output/cleaned_sensor_data")

    duration = time.time() - batch_start
    snapshot = STATE.analytics_snapshot()
    log_entry = {
        "batch_id": batch_id,
        "raw_rows": len(raw_rows),
        "cleaned_rows": len(cleaned_rows),
        "batch_duration_sec": round(duration, 3),
        "running_totals": {
            "missing_corrected": STATE.missing_corrected,
            "duplicates_removed": STATE.duplicates_removed,
            "invalid_removed": STATE.invalid_removed,
            "dropped_insufficient_history": STATE.dropped_insufficient_history,
            "late_accepted": STATE.late_accepted,
        },
        "analytics": snapshot,
    }
    STATE.batch_log.append(log_entry)
    print(f"\n--- [cleaning_query] batch {batch_id} | raw={len(raw_rows)} cleaned={len(cleaned_rows)} "
          f"duration={duration:.2f}s ---")
    print(
        f"  active_sensors={snapshot['active_sensor_count']}  status_dist={snapshot['status_distribution']}")


# ----------------------------------------------------------------------
# StreamingQueryListener - captures official Spark metrics (Performance Analysis)
# ----------------------------------------------------------------------
class MetricsListener(StreamingQueryListener):
    def __init__(self):
        # query name -> list of progress dicts
        self.progress_log = defaultdict(list)

    def onQueryStarted(self, event):
        print(f"Query started: {event.name} ({event.id})")

    def onQueryProgress(self, event):
        p = event.progress
        entry = {
            "timestamp": p.timestamp,
            "batchId": p.batchId,
            "inputRowsPerSecond": p.inputRowsPerSecond,
            "processedRowsPerSecond": p.processedRowsPerSecond,
            "durationMs": dict(p.durationMs) if p.durationMs else {},
            "numInputRows": p.numInputRows,
        }
        if p.stateOperators:
            entry["stateOperators"] = [
                {
                    "numRowsTotal": so.numRowsTotal,
                    "numRowsUpdated": so.numRowsUpdated,
                    "memoryUsedBytes": so.memoryUsedBytes,
                    "numRowsDroppedByWatermark": getattr(so, "numRowsDroppedByWatermark", None),
                }
                for so in p.stateOperators
            ]
        self.progress_log[p.name or str(p.id)].append(entry)

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")


# ----------------------------------------------------------------------
# Optional: keep feeding the topic with fresh producer batches during the demo
# ----------------------------------------------------------------------
def feeder_thread_fn(topic, bootstrap_servers, stop_event, records_per_burst, rate):
    import subprocess
    import sys
    while not stop_event.is_set():
        subprocess.run(
            [sys.executable, "-m", "assignment1.producer",
             "--topic", topic, "--records", str(records_per_burst),
             "--rate", str(rate), "--bootstrap-servers", bootstrap_servers],
            capture_output=True, text=True,
        )
        stop_event.wait(5)


def main():
    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming - Assignment 1 Part B")
    parser.add_argument("--rollno", required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--run-seconds", type=int,
                        default=240, help="how long to run the demo")
    parser.add_argument("--trigger-seconds", type=int,
                        default=15, help="micro-batch trigger interval")
    parser.add_argument("--feed", action="store_true",
                        help="keep producing fresh data during the run")
    parser.add_argument("--feed-records", type=int, default=300)
    parser.add_argument("--feed-rate", type=float, default=50.0)
    args = parser.parse_args()

    topic = f"sensor_{args.rollno}"
    os.makedirs("reports", exist_ok=True)
    os.makedirs("output/cleaned_sensor_data", exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("DA5402W-Assignment1-PartB")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    listener = MetricsListener()
    spark.streams.addListener(listener)

    # optional background feeder so the stream has continuous data during the demo
    stop_event = threading.Event()
    feeder = None
    if args.feed:
        feeder = threading.Thread(
            target=feeder_thread_fn,
            args=(topic, args.bootstrap_servers, stop_event,
                  args.feed_records, args.feed_rate),
            daemon=True,
        )
        feeder.start()
        print(
            f"Feeder thread started: publishing {args.feed_records} records every ~5s to '{topic}'")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("data"))
        .select("data.*")
        # Task 6: convert timestamp string -> Spark Timestamp (null if unparseable)
        .withColumn("event_time", expr("try_to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss')"))
    )

    print("\n=== Task 2: Parsed Stream Schema ===")
    parsed.printSchema()

    # ---------------- Query A: cleaning pipeline (Tasks 1-11) ----------------
    query_a = (
        parsed.writeStream
        .foreachBatch(clean_batch)
        .queryName("cleaning_query")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .option("checkpointLocation", "checkpoints/cleaning_query")
        .start()
    )

    # ---------------- Query B: genuine watermark + window demo (Tasks 12-14) ----------------
    watermarked = (
        parsed
        .filter(col("event_time").isNotNull())
        .filter(col("temperature").isNotNull())
        .filter((col("temperature") >= -20) & (col("temperature") <= 100))
        .withWatermark("event_time", "5 minutes")
        .dropDuplicatesWithinWatermark(["sensor_id", "timestamp"])
    )

    windowed = (
        watermarked
        .groupBy(window(col("event_time"), "5 minutes"), col("sensor_id"))
        .agg(
            spark_avg("temperature").alias("avg_temperature"),
            spark_count("*").alias("record_count"),
        )
    )

    query_b = (
        windowed.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .queryName("windowed_query")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .option("checkpointLocation", "checkpoints/windowed_query")
        .start()
    )

    print(f"\nStreaming started. Running for {args.run_seconds} seconds...\n")
    time.sleep(args.run_seconds)

    print("\nStopping feeder and streaming queries...")
    stop_event.set()
    query_a.stop()
    query_b.stop()
    query_a.awaitTermination(30)
    query_b.awaitTermination(30)

    # ---------------- Final reporting ----------------
    dropped_by_watermark = 0
    state_store_snapshot = None
    for entry in listener.progress_log.get("windowed_query", []):
        for so in entry.get("stateOperators", []):
            if so.get("numRowsDroppedByWatermark"):
                dropped_by_watermark += so["numRowsDroppedByWatermark"]
            state_store_snapshot = so  # keep the latest

    summary_table = STATE.summary_table()
    summary_table["Records Discarded by Watermarking"] = dropped_by_watermark

    perf_a = [
        {"batchId": e["batchId"], "inputRowsPerSecond": e["inputRowsPerSecond"],
         "processedRowsPerSecond": e["processedRowsPerSecond"], "durationMs": e["durationMs"]}
        for e in listener.progress_log.get("cleaning_query", [])
    ]
    perf_b = [
        {"batchId": e["batchId"], "inputRowsPerSecond": e["inputRowsPerSecond"],
         "processedRowsPerSecond": e["processedRowsPerSecond"], "durationMs": e["durationMs"]}
        for e in listener.progress_log.get("windowed_query", [])
    ]

    with open("reports/spark_summary_table.json", "w") as f:
        json.dump(summary_table, f, indent=2)
    with open("reports/spark_analytics.json", "w") as f:
        json.dump(STATE.analytics_snapshot(), f, indent=2)
    with open("reports/spark_performance.json", "w") as f:
        json.dump({
            "cleaning_query_progress": perf_a,
            "windowed_query_progress": perf_b,
            "state_store_snapshot_windowed_query": state_store_snapshot,
        }, f, indent=2)
    with open("reports/spark_batch_log.json", "w") as f:
        json.dump(STATE.batch_log, f, indent=2, default=str)

    print("\n=== Summary Table ===")
    print(json.dumps(summary_table, indent=2))
    print("\n=== Final Analytics ===")
    print(json.dumps(STATE.analytics_snapshot(), indent=2))
    print("\nAll reports written to reports/spark_*.json")

    spark.stop()


if __name__ == "__main__":
    main()
