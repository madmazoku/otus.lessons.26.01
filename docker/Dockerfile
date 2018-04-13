FROM ubuntu:trusty
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 379CE192D401AB61
RUN echo "deb http://dl.bintray.com/madmazoku/otus.lessons trusty main" | sudo tee -a /etc/apt/sources.list
RUN apt update
RUN apt install -y join_server