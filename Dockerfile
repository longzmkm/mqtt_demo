FROM python:3.6-alpine
RUN mkdir -p /home/newland/smart_home_server
WORKDIR /home/newland/smart_home_server
COPY ./code/ /home/newland/smart_home_server
RUN pip install ./code/requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
EXPOSE 1883/tcp
CMD ["python", "./code/manager.py"]