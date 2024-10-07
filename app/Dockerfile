# root builder stage
FROM python:3.9-slim AS builder
WORKDIR /app

COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r ./requirements.txt
COPY ./main.py /app/.
COPY ./etl /app/etl


# test stage
FROM builder AS test
COPY ./requirements.test.txt /app/.
RUN pip install --no-cache-dir -r /app/requirements.test.txt
COPY ./tests /app/tests
ENV PYTEST_ADDOPTS="--color=yes"
CMD ["pytest", "-svv", "/app/tests/"]

# prod stage
FROM builder AS prod
COPY --from=builder /app/main.py /app/.
COPY --from=builder /app/etl /app/etl
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python3", "/app/main.py"]
