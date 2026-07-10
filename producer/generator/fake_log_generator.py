"""실제 API 서비스를 흉내 내는 Fake Log Generator."""

import random
from dataclasses import dataclass

from common.log_event import LogEvent


@dataclass(frozen=True)
class LogTemplate:
    """서비스에서 발생할 수 있는 로그 패턴 하나를 정의한다."""

    level: str
    category: str
    message: str
    method: str
    path: str
    status_code: int
    min_latency_ms: int
    max_latency_ms: int
    weight: int


class FakeLogGenerator:
    """가상의 여러 서비스에서 애플리케이션 로그를 생성한다."""

    def __init__(self) -> None:
        self._templates: dict[str, list[LogTemplate]] = {
            "gateway": [
                LogTemplate(
                    level="INFO",
                    category="HTTP",
                    message="Request completed",
                    method="GET",
                    path="/api/products",
                    status_code=200,
                    min_latency_ms=20,
                    max_latency_ms=180,
                    weight=75,
                ),
                LogTemplate(
                    level="WARN",
                    category="HTTP",
                    message="Rate limit threshold exceeded",
                    method="POST",
                    path="/api/orders",
                    status_code=429,
                    min_latency_ms=5,
                    max_latency_ms=30,
                    weight=15,
                ),
                LogTemplate(
                    level="ERROR",
                    category="UPSTREAM",
                    message="Upstream service unavailable",
                    method="GET",
                    path="/api/payments",
                    status_code=503,
                    min_latency_ms=500,
                    max_latency_ms=2000,
                    weight=10,
                ),
            ],
            "auth-api": [
                LogTemplate(
                    level="INFO",
                    category="AUTH",
                    message="Login successful",
                    method="POST",
                    path="/auth/login",
                    status_code=200,
                    min_latency_ms=40,
                    max_latency_ms=250,
                    weight=80,
                ),
                LogTemplate(
                    level="WARN",
                    category="AUTH",
                    message="Invalid login credentials",
                    method="POST",
                    path="/auth/login",
                    status_code=401,
                    min_latency_ms=30,
                    max_latency_ms=180,
                    weight=15,
                ),
                LogTemplate(
                    level="ERROR",
                    category="DATABASE",
                    message="User database timeout",
                    method="POST",
                    path="/auth/login",
                    status_code=500,
                    min_latency_ms=900,
                    max_latency_ms=3000,
                    weight=5,
                ),
            ],
            "payment-api": [
                LogTemplate(
                    level="INFO",
                    category="PAYMENT",
                    message="Payment completed",
                    method="POST",
                    path="/payments",
                    status_code=201,
                    min_latency_ms=100,
                    max_latency_ms=700,
                    weight=75,
                ),
                LogTemplate(
                    level="WARN",
                    category="PAYMENT",
                    message="Payment provider response delayed",
                    method="POST",
                    path="/payments",
                    status_code=202,
                    min_latency_ms=700,
                    max_latency_ms=1500,
                    weight=15,
                ),
                LogTemplate(
                    level="ERROR",
                    category="PAYMENT",
                    message="Payment provider timeout",
                    method="POST",
                    path="/payments",
                    status_code=504,
                    min_latency_ms=1500,
                    max_latency_ms=5000,
                    weight=10,
                ),
            ],
        }

    def generate(self) -> LogEvent:
        """서비스와 로그 패턴을 무작위로 골라 LogEvent를 생성한다."""

        service = random.choice(list(self._templates))
        templates = self._templates[service]

        # weight 값이 클수록 해당 로그가 더 자주 선택된다.
        template = random.choices(
            templates,
            weights=[item.weight for item in templates],
            k=1,
        )[0]

        instance_number = random.randint(1, 3)

        return LogEvent.create(
            service=service,
            instance=f"{service}-{instance_number:02d}",
            level=template.level,
            category=template.category,
            message=template.message,
            method=template.method,
            path=template.path,
            status_code=template.status_code,
            latency_ms=random.randint(
                template.min_latency_ms,
                template.max_latency_ms,
            ),
            metadata={
                "environment": "local",
                "generator": "fake-log-generator",
            },
        )
