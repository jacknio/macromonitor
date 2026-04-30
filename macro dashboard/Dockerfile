FROM python:3.12-slim

WORKDIR /app
COPY . .

ENV HOST=0.0.0.0
ENV PORT=8787
EXPOSE 8787

CMD ["python3", "server.py"]
