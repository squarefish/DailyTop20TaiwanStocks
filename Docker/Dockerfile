FROM python:3.8-slim

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

ENV PORT=9000

EXPOSE 9000

CMD ["python", "access_top20_stocks_to_bq.py"]
