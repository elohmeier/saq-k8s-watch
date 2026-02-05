FROM python:3.14-slim

WORKDIR /app

RUN pip install --no-cache-dir "saq[redis]" "typing-extensions>=4.12.2"

CMD ["python", "-c", "import asyncio\nfrom saq.queue import Queue\nfrom saq.worker import Worker\n\nasync def burn():\n    data = []\n    while True:\n        data.append(bytearray(10 * 1024 * 1024))\n\nasync def main():\n    import os\n    queue = Queue.from_url(os.environ['SAQ_QUEUE_URL'], name='default')\n    await queue.connect()\n    worker = Worker(queue, functions=[], id=os.environ.get('SAQ_WORKER_ID'), burst=True, max_burst_jobs=1, dequeue_timeout=1)\n    asyncio.create_task(burn())\n    await worker.start()\n\nasyncio.run(main())\n"]
