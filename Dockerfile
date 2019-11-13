FROM maven:3-jdk-8 as build
MAINTAINER Burak Emre Kabakci "emre@rakam.io"

WORKDIR /var/app

ADD pom.xml pom.xml
ADD ./rakam/pom.xml rakam/pom.xml
ADD ./rakam-aws/pom.xml rakam-aws/pom.xml
ADD ./rakam-postgresql/pom.xml rakam-postgresql/pom.xml
ADD ./rakam-presto/pom.xml rakam-presto/pom.xml
ADD ./rakam-presto-kafka/pom.xml rakam-presto-kafka/pom.xml
ADD ./rakam-spi/pom.xml rakam-spi/pom.xml
ADD ./mapper/rakam-mapper-geoip-ip2location/pom.xml mapper/rakam-mapper-geoip-ip2location/pom.xml
ADD ./mapper/rakam-mapper-geoip-maxmind/pom.xml mapper/rakam-mapper-geoip-maxmind/pom.xml
ADD ./mapper/rakam-mapper-website/pom.xml mapper/rakam-mapper-website/pom.xml
RUN mvn verify clean --fail-never

ADD ./rakam/ rakam
ADD ./rakam-aws/ rakam-aws
ADD ./rakam-postgresql/ rakam-postgresql
ADD ./rakam-presto/ rakam-presto
ADD ./rakam-presto-kafka/ rakam-presto-kafka
ADD ./rakam-spi/ rakam-spi
ADD ./mapper/rakam-mapper-geoip-ip2location/ mapper/rakam-mapper-geoip-ip2location
ADD ./mapper/rakam-mapper-geoip-maxmind/ mapper/rakam-mapper-geoip-maxmind
ADD ./mapper/rakam-mapper-website/ mapper/rakam-mapper-website
RUN mvn package -T 1C -DskipTests=true

RUN apt-get update \
    # Rakam can automatically download & extract the database but we do this
    # at compile time of the container because it increases the start time of the containers.
    && wget -P /tmp http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz \
    && gzip -d /tmp/GeoLite2-City.mmdb.gz

# Make environment variable active
RUN cd /var/app/rakam/target/rakam-*-bundle/rakam-*/ && \
    mkdir etc && \
    echo '\n-Denv=RAKAM_CONFIG' >> ./etc/jvm.config

FROM openjdk:8-jre
COPY --from=build /var/app/rakam/target/ /rtmp
COPY --from=build /tmp/GeoLite2-City.mmdb /tmp/GeoLite2-City.mmdb
ADD ./entrypoint.sh /app/entrypoint.sh

RUN cp -r /rtmp/rakam-*-bundle/rakam-*/* /app/ && \
    chmod +x /app/entrypoint.sh && \
    rm -rf /rtmp

ENTRYPOINT ["/app/entrypoint.sh"]
EXPOSE 9999