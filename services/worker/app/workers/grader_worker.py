import os, json, time
from typing import Dict, Any, List, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import boto3
from botocore.exceptions import ClientError

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_GRADE_REQ = os.getenv("TOPIC_GRADE_REQ", "quizzes.grade")
TOPIC_GRADE_DONE = os.getenv("TOPIC_GRADE_DONE", "quizzes.graded")

S3_ENDPOINT = (
    os.getenv("S3_ENDPOINT")
    or os.getenv("MINIO_ENDPOINT")
    or "http://minio:9000"
)
S3_ACCESS = (
    os.getenv("S3_ACCESS_KEY")
    or os.getenv("MINIO_ACCESS_KEY")
    or os.getenv("MINIO_ROOT_USER")
    or "minioadmin"
)
S3_SECRET = (
    os.getenv("S3_SECRET_KEY")
    or os.getenv("MINIO_SECRET_KEY")
    or os.getenv("MINIO_ROOT_PASSWORD")
    or "minioadmin"
)
S3_BUCKET = (
    os.getenv("S3_BUCKET")
    or os.getenv("MINIO_BUCKET")
    or "assistant-teacher"
)

s3 = boto3.resource(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS,
    aws_secret_access_key=S3_SECRET,
)


def _s3_key(*parts: str) -> str:
    return "/".join(parts)


def _load_quiz(job_id: str) -> Optional[Dict[str, Any]]:
    key = _s3_key("quizzes", job_id, "quiz.json")
    try:
        body = s3.Object(S3_BUCKET, key).get()["Body"].read()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "NoSuchKey":
            print(f"no quiz for job {job_id}, skip")
            return None
        raise
    return json.loads(body)


def _save_grade(grade_job_id: str, result: Dict[str, Any]):
    s3.Object(S3_BUCKET, _s3_key("grades", grade_job_id, "result.json")).put(
        Body=json.dumps(result, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    s3.Object(S3_BUCKET, _s3_key("grades", grade_job_id, "status.json")).put(
        Body=json.dumps({"status": "ready"}).encode("utf-8"),
        ContentType="application/json",
    )


def score(quiz: Dict[str, Any], answers: List[Any]) -> Dict[str, Any]:
    right = 0
    details = []
    items = quiz.get("items", [])
    for i, it in enumerate(items):
        a = answers[i] if i < len(answers) else None
        if it.get("type") == "cloze":
            ok = str(a).strip().lower() == str(it.get("answer", "")).strip().lower()
        elif it.get("type") == "tf":
            ok = bool(a) == bool(it.get("answer"))
        else:
            ok = str(a).strip().lower() == str(it.get("answer", "")).strip().lower()
        right += 1 if ok else 0
        details.append({"q": it.get("question"), "given": a, "ok": ok})
    return {"right": right, "total": len(items), "details": details}


def make_producer() -> KafkaProducer:
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            )
            return p
        except NoBrokersAvailable as e:
            print(f"Kafka not available: {e}, retry in 5s")
            time.sleep(5)


def make_consumer() -> KafkaConsumer:
    while True:
        try:
            c = KafkaConsumer(
                TOPIC_GRADE_REQ,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="quiz-grader",
                enable_auto_commit=True,
            )
            return c
        except NoBrokersAvailable as e:
            print(f"Kafka not available: {e}, retry in 5s")
            time.sleep(5)


def run():
    p = make_producer()
    c = make_consumer()
    for msg in c:
        v = msg.value
        quiz_job = v["job_id"]
        ans = v.get("answers", [])
        grade_job = v.get("grade_job_id")
        q = _load_quiz(quiz_job)
        if q is None:
            continue
        res = score(q, ans)
        _save_grade(grade_job, res)
        p.send(TOPIC_GRADE_DONE, {"job_id": grade_job, "status": "ready"})
        p.flush()


if __name__ == "__main__":
    run()
