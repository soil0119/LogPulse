"""Fake Log Generator와 Kafka Publisher를 연결하는 실행 진입점."""

import logging
import os
import time

from dotenv import load_dotenv

from producer.generator.fake_log_generator import FakeLogGenerator
from producer.kafka.publisher import KafkaLogPublisher


def configure_logging() -> None:
    """터미널 로그 출력 형식을 설정한다."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> None:
    """가짜 로그를 반복 생성해 Kafka로 전송한다."""

    # 프로젝트 루트의 .env 파일을 읽는다.
    load_dotenv()
    configure_logging()

    logger = logging.getLogger(__name__)

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )
    topic = os.getenv("KAFKA_TOPIC", "app-logs")
    interval_seconds = float(
        os.getenv("LOG_INTERVAL_SECONDS", "1.0")
    )

    generator = FakeLogGenerator()
    publisher = KafkaLogPublisher(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )

    logger.info(
        "LogPulse Producer 시작 broker=%s topic=%s interval=%s초",
        bootstrap_servers,
        topic,
        interval_seconds,
    )

    try:
        while True:
            # 가짜 로그 하나를 생성한다.
            event = generator.generate()

            # 생성된 로그를 Kafka에 전송한다.
            publisher.publish(event)

            logger.info(
                "로그 생성 event_id=%s service=%s level=%s status=%d",
                event.event_id,
                event.service,
                event.level,
                event.status_code,
            )

            # 설정된 시간만큼 기다린 뒤 다음 로그를 생성한다.
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("Producer 종료 요청을 받았습니다.")

    finally:
        # 종료 전에 Kafka 전송 버퍼를 비운다.
        publisher.close()
        logger.info("Producer가 정상 종료되었습니다.")


if __name__ == "__main__":
    main()
