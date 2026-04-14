FROM python:3.12.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN useradd --create-home --shell /bin/sh froguser

COPY pyproject.toml README.md CONTRIBUTING.md IMPLEMENTATION_PLAN.md LICENSE ./
COPY src ./src
COPY templates ./templates
COPY static ./static
COPY wednesday-frog.png ./wednesday-frog.png
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN python -m pip install --upgrade pip && python -m pip install . && chmod +x /usr/local/bin/docker-entrypoint.sh

RUN mkdir -p /data && chown -R froguser:froguser /app /data

EXPOSE 8000

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["wednesday-frog", "serve", "--host", "0.0.0.0", "--port", "8000"]
