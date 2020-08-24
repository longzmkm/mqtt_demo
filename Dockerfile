FROM python:3.7-alpine
ENV PYTHONUNBUFFERED 0
WORKDIR /code
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
COPY . .
CMD ["python","./code/manage.py"]