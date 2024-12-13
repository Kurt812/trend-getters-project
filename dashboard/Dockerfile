FROM python:3.12

COPY requirements.txt . 

RUN pip3 install -r requirements.txt

RUN mkdir ./pages

COPY ./pages ./pages

COPY combined_data.py .

COPY predict_mentions.py .

COPY queries.py .

COPY Home.py .

EXPOSE 8501

CMD ["streamlit", "run", "Home.py", "--server.port=8501"]