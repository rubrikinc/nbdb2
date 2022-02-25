FROM tiangolo/uwsgi-nginx-flask:python3.7

RUN apt-get update -y && \
    apt-get install -y python3-pip

# We copy the dependencies names first to leverage Docker cache
COPY ./setup.py /app/setup.py
COPY ./setup_pypy.py /app/setup_pypy.py
COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

# TODO Move pypy to artifact
RUN wget "https://downloads.python.org/pypy/pypy3.7-v7.3.2-linux64.tar.bz2" && \
    tar xjf *.bz2 && \
    cd "pypy3.7-v7.3.2-linux64" && \
    ln -s ${PWD}/bin/pypy /usr/bin/pypy3 && \
    pypy3 -m ensurepip && \
    pypy3 -mpip install -U pip wheel

RUN pip3 install setuptools
RUN python3 setup.py install
RUN pypy3 setup_pypy.py install
RUN pip3 install -r requirements.txt

COPY . /app

EXPOSE 5000
