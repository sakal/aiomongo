FROM python:3.5
RUN pip install -U pip wheel
RUN pip install -U setuptools
ADD ./dev-requirements.txt ./
RUN pip install -r dev-requirements.txt
