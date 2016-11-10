FROM python:3.5
RUN mkdir /code
WORKDIR /code
ADD dev-requirements.txt /code/
RUN pip install -U pip
RUN pip install -r dev-requirements.txt
ADD . /code/