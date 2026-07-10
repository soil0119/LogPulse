"""Kafka의 app-logs Topic을 읽어 터미널에 출력하는 Consumer."""

import json
import logging
import os

from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv


def configure_logging() -> None:
    """Consumer 로그 출력 형식을 설정한다."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> None:
    """Kafka 메시지를 읽고 성공적으로 처리한 후 Offset을 커밋한다."""

    load_dotenv()
    configure_logging()

    logger = logging.getLogger(__name__)

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )
    topic = os.getenv("KAFKA_TOPIC", "app-logs")
    group_id = os.getenv(
        "KAFKA_CONSUMER_GROUP",
        "logpulse-console-consumer",
    )

    consumer = Consumer(
        {
            # Kafka Broker 주소
            "bootstrap.servers": bootstrap_servers,

            # 동일한 group.id를 사용하는 Consumer들은 Partition을 나눠 처리한다.
            "group.id": group_id,

            # 저장된 Offset이 없으면 Topic 처음부터 읽는다.
            "auto.offset.reset": "earliest",

            # 자동 Offset 커밋을 끈다.
            "enable.auto.commit": False,
        }
    )

    # app-logs Topic을 구독한다.
    consumer.subscribe([topic])

    logger.info(
        "LogPulse Consumer 시작 broker=%s topic=%s group=%s",
        bootstrap_servers,
        topic,
        group_id,
    )

    try:
        while True:
            # Kafka에서 메시지를 최대 1초 동안 기다린다.
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                raise KafkaException(message.error())

            try:
                # Kafka 메시지의 bytes 값을 UTF-8 문자열로 바꾸고 JSON으로 파싱한다.
                event = json.loads(
                    message.value().decode("utf-8")
                )

                print(
                    f"[{event['timestamp']}] "
                    f"{event['level']:<5} "
                    f"{event['service']:<12} "
                    f"{event['method']} {event['path']} "
                    f"status={event['status_code']} "
                    f"latency={event['latency_ms']}ms "
                    f"message={event['message']}"
                )

                # 메시지 처리가 성공한 뒤에만 Offset을 저장한다.
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
                KeyError,
            ) as exc:
                logger.exception(
                    "메시지 처리 실패 "
                    "topic=%s partition=%d offset=%d error=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    exc,
                )

                # 실패한 메시지는 커밋하지 않는다.
                # Consumer가 재시작되면 다시 처리할 수 있다.

    except KeyboardInterrupt:
        logger.info("Consumer 종료 요청을 받았습니다.")

    finally:
        # Consumer Group에서 정상적으로 나가고 Partition을 반납한다.
        consumer.close()
        logger.info("Consumer가 정상 종료되었습니다.")


if __name__ == "__main__":
    main()
