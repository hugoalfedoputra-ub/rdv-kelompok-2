# syntax=docker/dockerfile:1

FROM python:3.13-alpine

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Create a non-privileged user that the app will run under.
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser

# Download dependencies as a separate step to take advantage of Docker's caching.
# Source path for requirements.txt is now relative to the build context root.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python -m pip install -r requirements.txt

# Switch to the non-privileged user.
USER appuser

# Copy the necessary files into the /app directory in the container.
# Paths are relative to the build context.
COPY etl_kafka.py /app/etl_kafka.py
COPY etl_all.py /app/etl_all.py

# Run the application.
CMD ["python3", "etl_all.py"]

# Example build command
# docker build -t weather-kafka-etl:1.0 -f Dockerfile . --no-cache