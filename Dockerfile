FROM python:3.14-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock README.md /app/
COPY saq_k8s_watch /app/saq_k8s_watch
COPY docker_entrypoint.py /app/docker_entrypoint.py

RUN pip install --no-cache-dir .

CMD ["python", "/app/docker_entrypoint.py"]
