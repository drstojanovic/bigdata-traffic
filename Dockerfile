FROM python:2.7

ENV SUMO_VERSION 0.31.0
ENV SUMO_HOME /opt/sumo

RUN apt-get update &&              \
  apt-get install -y             \
    build-essential              \
    git                          \
    libxerces-c-dev \
    wget \
    g++ \
    make \
    libfox-1.6-0 libfox-1.6-dev \
    python2.7 \
    libproj-dev \
    proj-bin

RUN mkdir -p /opt
#RUN (cd /opt; git clone https://github.com/radiganm/sumo.git)
RUN wget http://downloads.sourceforge.net/project/sumo/sumo/version%20$SUMO_VERSION/sumo-src-$SUMO_VERSION.tar.gz
RUN tar xzf sumo-src-$SUMO_VERSION.tar.gz && \
    mv sumo-$SUMO_VERSION $SUMO_HOME && \
    rm sumo-src-$SUMO_VERSION.tar.gz
RUN cd $SUMO_HOME && ./configure && make install
# RUN (cd /opt; ./configure)
# RUN (cd /opt; make)
# RUN (cd /opt; make install)

#ENV SUMO_HOME /opt
# First cache dependencies
ADD ./setup.py /app/setup.py
RUN python /app/setup.py install
# Add sources
ADD ./ /app/
WORKDIR /app
CMD ["python","/app/forever.py"]