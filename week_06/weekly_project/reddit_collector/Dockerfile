FROM python:3.9

WORKDIR /app

COPY requirements.txt /app
COPY get_reddits.py /app

RUN pip install -r requirements.txt

CMD ["python", "get_reddits.py"]