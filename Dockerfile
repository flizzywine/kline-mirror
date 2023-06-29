
# Use an official Python runtime as the base image
# FROM python:3.9-slim-buster 一会调试完成再用
FROM python:3.9-slim-buster
# Set the working directory inside the container
WORKDIR /app

# 把应用的依赖包 全部下载下来
COPY requirements.txt .

RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/ \
        && pip install -r requirements.txt

EXPOSE 8501
