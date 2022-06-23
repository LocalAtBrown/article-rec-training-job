# if this python version changes, the python version in pyproject.toml needs to change too
FROM python:3.8.5-slim

WORKDIR /app

# install requirements
COPY ./requirements.txt /app/

RUN apt-get update && apt-get install -y git
RUN pip install --upgrade pip && pip install -r /app/requirements.txt
RUN pip install -f https://download.pytorch.org/whl/torch_stable.html torch==1.10.0+cpu
RUN pip install spotlight@git+https://github.com/maciejkula/spotlight.git@master

COPY . /app/

CMD ["python", "/app/app.py"]
