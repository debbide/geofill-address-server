FROM python:3.12-slim

WORKDIR /srv/app
COPY server.py /srv/app/server.py
COPY data/addresses.db /srv/app/data/addresses.db

ENV ADDRESS_DB=/srv/app/data/addresses.db \
    API_HOST=0.0.0.0 \
    API_PORT=8788

EXPOSE 8788

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8788/health')"

CMD ["python3", "server.py"]
