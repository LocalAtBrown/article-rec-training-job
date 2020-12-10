FROM python:3.8.5-slim

WORKDIR /app

# install dependenciesa
RUN apt-get update && apt-get -y install curl

# install requirements
COPY ./requirements.txt /app/
COPY ./setup.py /app/
COPY ./lib/__init__.py /app/lib/__init__.py

RUN pip install --upgrade pip && pip install -r /app/requirements.txt

COPY . /app/

ENTRYPOINT ["/app/entrypoint"]

CMD ["python", "/app/app.py"]
