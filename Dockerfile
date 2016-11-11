FROM python:3.5
RUN pip install -U pip
ADD ./dev-requirements.txt ./
RUN pip install -r dev-requirements.txt
