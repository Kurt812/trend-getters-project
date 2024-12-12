FROM python:3.12

COPY requirements.txt . 

RUN pip3 install -r requirements.txt

COPY notify_pipeline.py .

CMD ["python", "notify_pipeline.py"]