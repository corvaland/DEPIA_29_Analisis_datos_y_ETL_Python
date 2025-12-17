# Referenciar una imagen Base de Python - Ubuntu
FROM ubuntu:20.04

# evitar que el SO terire alguna instlacion manual
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    wget && \
    apt-get clean

RUN pip3 install --upgrade pip && \
    pip3 install notebook==6.5.6

WORKDIR /worksapace

RUN mkdir -p /root/.jupyter

RUN echo "c.NotebookApp.token = ''" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.password = ''" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.open_browser = False" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.allow_root = True" >> /root/.jupyter/jupyter_notebook_config.py 


EXPOSE 8888

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888",  "--no-browser", "--allow-root"]