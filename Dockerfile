FROM maven:3-jdk-8
MAINTAINER Burak Emre Kabakci "emre@rakam.io"

WORKDIR /var/app

RUN git clone https://github.com/rakam-io/rakam.git
RUN cd rakam && mvn install -DskipTests

RUN echo 'org.rakam=INFO\n\
io.netty=INFO' > log.properties


RUN apt-get update \
    # Rakam can automatically download & extract the database but we do this
    # at compile time of the container because it increases the start time of the containers.
    && wget -P /tmp http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz \
    && gzip -d /tmp/GeoLite2-City.mmdb.gz

# Make environment variable active
RUN cd /var/app/rakam/rakam/target/rakam-*-bundle/rakam-*/etc/ && echo '\n-Denv=RAKAM_CONFIG' >> jvm.config

WORKDIR /var/app/rakam

EXPOSE 9999

ENTRYPOINT rakam/target/rakam-*-bundle/rakam-*/bin/launcher run --config ../config.properties

RUN apt-get clean
