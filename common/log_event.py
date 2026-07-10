"""LogPulse에서 Producer와 Consumer가 공유하는 로그 이벤트 모델."""

from dataclasses import asdict, dataclass

from datetime import datetime, timezone

from typing import Any

from uuid import uuid4

@dataclass(frozen=True)

class LogEvent:

    """Kafka의 app-logs Topic으로 전달되는 로그 이벤트 형식."""

    event_id: str

    service: str

    instance: str

    level: str

    category: str

    message: str

    method: str

    path: str

    status_code: int

    latency_ms: int

    timestamp: str

    metadata: dict[str, Any]

    @classmethod

    def create(

        cls,

        *,

        service: str,

        instance: str,

        level: str,

        category: str,

        message: str,

        method: str,

        path: str,

        status_code: int,

        latency_ms: int,

        metadata: dict[str, Any] | None = None,

    ) -> "LogEvent":

        """

        새로운 로그 이벤트를 생성한다.

        event_id와 timestamp는 호출하는 코드가 직접 만들지 않고

        이 메서드가 자동으로 생성한다.

        """

        return cls(

            event_id=str(uuid4()),

            service=service,

            instance=instance,

            level=level,

            category=category,

            message=message,

            method=method,

            path=path,

            status_code=status_code,

            latency_ms=latency_ms,

            timestamp=datetime.now(timezone.utc).isoformat(),

            metadata=metadata or {},

        )

    def to_dict(self) -> dict[str, Any]:

        """LogEvent 객체를 JSON 변환 가능한 딕셔너리로 바꾼다."""

        return asdict(self)

