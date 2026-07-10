"""LogEvent를 Kafka Topic으로 전송하는 Publisher."""

import json
import logging

from confluent_kafka import KafkaException, Producer

from common.log_event import LogEvent

logger = logging.getLogger(__name__)


class KafkaLogPublisher:
    """LogEvent를 JSON으로 직렬화해 Kafka에 전송한다."""

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic

        self._producer = Producer(
            {
                # Kafka Broker 주소
                "bootstrap.servers": bootstrap_servers,

                # Kafka가 메시지를 정상 기록했다고 확인할 때까지 기다린다.
                "acks": "all",

                # 일시적 실패가 발생하면 최대 5회 재시도한다.
                "retries": 5,

                # 재시도 과정의 중복 메시지 발생 가능성을 줄인다.
                "enable.idempotence": True,
            }
        )

    def publish(self, event: LogEvent) -> None:
        """LogEvent 하나를 Kafka로 비동기 전송한다."""

        payload = json.dumps(
            event.to_dict(),
            ensure_ascii=False,
        ).encode("utf-8")

        try:
            self._producer.produce(
                topic=self._topic,

                # 같은 서비스의 로그가 같은 Partition으로 가도록 사용한다.
                key=event.service.encode("utf-8"),

                # Kafka에 실제로 저장되는 JSON 데이터
                value=payload,

                # 전송 결과를 처리할 콜백 함수
                on_delivery=self._on_delivery,
            )

            # 완료된 전송 콜백을 처리한다.
            self._producer.poll(0)

        except BufferError as exc:
            raise RuntimeError(
                "Kafka Producer 내부 버퍼가 가득 찼습니다."
            ) from exc

        except KafkaException as exc:
            raise RuntimeError(
                "Kafka 메시지 전송 요청에 실패했습니다."
            ) from exc

    def close(self) -> None:
        """프로그램 종료 전에 버퍼에 남은 메시지를 전송한다."""

        remaining = self._producer.flush(timeout=10)

        if remaining > 0:
            logger.warning(
                "종료 전 전송하지 못한 메시지가 %d개 남았습니다.",
                remaining,
            )

    @staticmethod
    def _on_delivery(error, message) -> None:
        """Kafka가 전송 결과를 반환하면 실행된다."""

        if error is not None:
            logger.error("Kafka 전송 실패: %s", error)
            return

        logger.info(
            "전송 성공 topic=%s partition=%d offset=%d",
            message.topic(),
            message.partition(),
            message.offset(),
        )
