FROM python:3.8-slim
RUN ln -sf /usr/share/zoneinfo/Europe/Moscow /etc/localtime
COPY requirements.txt /app/
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
CMD ["python","main.py"]