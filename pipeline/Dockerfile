FROM python:latest

COPY requirements.txt . 

RUN pip3 install -r requirements.txt

COPY load.py . 

COPY api.py .

EXPOSE 5000

CMD ["python", "api.py"]