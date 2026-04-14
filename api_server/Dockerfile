# Image
FROM python:3.12.12-slim

# Work Dir
WORKDIR /app

# Requir Lib
RUN pip install fastapi pydantic uvicorn

COPY main.py .

CMD [ "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]