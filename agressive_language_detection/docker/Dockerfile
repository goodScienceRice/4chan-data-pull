FROM jupyter/scipy-notebook

WORKDIR /app
#USER root
#RUN apt-get update && apt-get -y install rsync git

#USER jovyan
COPY requirements.txt .
RUN pip install -r requirements.txt

EXPOSE 8888
EXPOSE 7860
