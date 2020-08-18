FROM python:3.7-alpine
RUN mkdir /api
COPY ./code/ /api/
WORKDIR /api
RUN pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
CMD ["python", "manage.py"]
