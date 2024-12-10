FROM python:3.12

COPY requirements.txt . 

RUN pip3 install -r requirements.txt

COPY extract.py .

COPY transform.py .

COPY load.py . 

COPY etl.py .

COPY api.py .

EXPOSE 5000

CMD ["python", "api.py"]