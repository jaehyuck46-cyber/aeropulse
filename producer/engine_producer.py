"""C-MAPSS(FD001) 엔진 센서 데이터를 Kafka로 전송하는 Producer.

Phase 1 EDA에서 선별한 센서 14개만 사용하며, 사이클 순서대로
engine-sensors 토픽에 전송해 실제 비행처럼 실시간 흐름을 재현한다.
메시지 key는 엔진 ID(unit) → 같은 엔진은 항상 같은 파티션으로 간다.
"""
import argparse
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

SELECTED_SENSORS = [
    "s2", "s3", "s4", "s7", "s8", "s9", "s11", "s12",
    "s13", "s14", "s15", "s17", "s20", "s21",
]

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "train_FD001.txt"
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "engine-sensors"
CYCLE_INTERVAL_SECONDS = 0.5


def load_data() -> pd.DataFrame:
    cols = ["unit", "cycle", "op1", "op2", "op3"] + [f"s{i}" for i in range(1, 22)]
    df = pd.read_csv(DATA_PATH, sep=r"\s+", header=None, names=cols)
    return df[["unit", "cycle"] + SELECTED_SENSORS]


def main(max_cycles: int | None) -> None:
    df = load_data()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    max_cycle = int(df["cycle"].max())
    if max_cycles is not None:
        max_cycle = min(max_cycle, max_cycles)

    sent = 0
    for cycle in range(1, max_cycle + 1):
        rows = df[df["cycle"] == cycle]
        for _, row in rows.iterrows():
            unit = int(row["unit"])
            message = {"unit": unit, "cycle": int(row["cycle"])}
            message.update(row[SELECTED_SENSORS].to_dict())
            producer.send(TOPIC, key=str(unit), value=message)
            sent += 1
        producer.flush()
        print(f"cycle {cycle}/{max_cycle} 전송 완료 (엔진 {len(rows)}대, 누적 {sent}건)")
        time.sleep(CYCLE_INTERVAL_SECONDS)

    producer.close()
    print(f"총 {sent}건 전송 완료")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="C-MAPSS 엔진 센서 데이터를 Kafka로 전송")
    parser.add_argument(
        "--max-cycles", type=int, default=None,
        help="전송할 최대 사이클 수 (테스트용, 기본값: 전체)",
    )
    args = parser.parse_args()
    main(args.max_cycles)
