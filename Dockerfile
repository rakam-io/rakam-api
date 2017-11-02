FROM maven:3-jdk-8
MAINTAINER Burak Emre Kabakci "emre@rakam.io"

WORKDIR /var/app

RUN git clone https://github.com/rakam-io/rakam.git
RUN cd rakam && mvn install -DskipTests

RUN echo 'org.rakam=INFO\n\
io.netty=INFO' > log.properties

RUN [ -s config.properties ] || (echo "store.adapter=postgresql\n\
plugin.user.enabled=true\n\
event-explorer.enabled=true\n\
custom-data-source.enabled=true\n\
user.funnel-analysis.enabled=true\n\
plugin.user.enable-user-mapping=true\n\
user.retention-analysis.enabled=true\n\
plugin.geoip.enabled=true\n\
plugin.user.storage=postgresql\n\
http.server.address=0.0.0.0:9999\n\
plugin.user.storage.identifier-column=id\n\
plugin.geoip.database.url=file://tmp/GeoLite2-City.mmdb\n" > config.properties)

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
