FROM python:3.8.5-slim

WORKDIR /app

# install requirements
COPY ./requirements.txt /app/
COPY ./setup.py /app/
COPY ./lib/__init__.py /app/lib/__init__.py

RUN pip install --upgrade pip && pip install -r /app/requirements.txt

COPY . /app/

CMD ["python", "/app/app.py"]
