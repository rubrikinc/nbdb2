FROM ubuntu:20.04

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update
RUN apt install -y wget curl gnupg

RUN curl -fsSL https://apt.releases.hashicorp.com/gpg | apt-key add -
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN apt install -y build-essential libev4 libev-dev libyaml-dev python3.7 python3-pip python-dev python3.7-dev software-properties-common locate
RUN apt-add-repository "deb [arch=$(dpkg --print-architecture)] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
RUN apt install -y terraform=0.11.14
RUN curl -Lo /usr/local/bin/ecs-cli-1.20.0 https://amazon-ecs-cli.s3.amazonaws.com/ecs-cli-linux-amd64-v1.20.0
# RUN rm /usr/local/bin/ecs-cli && ln -s /usr/local/bin/ecs-cli-1.20.0 /usr/local/bin/ecs-cli
RUN chmod +x /usr/local/bin/ecs-cli-1.20.0
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN python3.7 get-pip.py
RUN rm ./get-pip.py
# TODO consolidate all python module installation to use either pip or setuptools
RUN pip3 install boto3 docker-compose jinja2 orjson==3.5.3 pyyaml retrying sshtunnel pylint phabricator tdigest
RUN curl -sSL https://get.docker.com/ | sh
RUN apt install -y python3.6 sudo lsof git

## Install Oracle's JDK
# add oracle jdk repository
RUN add-apt-repository ppa:ts.sch.gr/ppa \
# accept oracle license
  && echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections \
  && echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections \
  && apt-get update \
# install oracle jdk 8 and make it default
  && apt-get -y install oracle-java8-installer \
  && apt-get -y install oracle-java8-set-default

# clean up
RUN apt-get clean all \
  && rm -rf /var/lib/apt/lists/*

RUN update-java-alternatives -v --set /usr/lib/jvm/java-8-oracle
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.6 1
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 2
RUN update-alternatives --config python3

RUN adduser --disabled-password \
--gecos '' ubuntu
#  Add new user docker to sudo group
RUN adduser ubuntu sudo
# Ensure sudo group users are not 
# asked for a password when using 
# sudo command by ammending sudoers file
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> \
/etc/sudoers

USER ubuntu
WORKDIR /home/ubuntu

# ENTRYPOINT ["/bin/bash"]
