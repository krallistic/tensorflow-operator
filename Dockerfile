FROM python:3.5

ADD requirements.txt requirements.txt

RUN pip install --upgrage -r requirements.txt

ENTRYPOINT ['python', 'operator.py']



