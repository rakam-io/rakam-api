FROM maven:3-jdk-8
MAINTAINER Burak Emre Kabakci "emrekabakci@gmail.com"

RUN apt-get update && apt-get install -y nodejs npm
RUN ln -s /usr/bin/nodejs /usr/bin/node
RUN echo '{ "allow_root": true }' > /root/.bowerrc

WORKDIR /var/app

RUN git clone https://github.com/buremba/rakam-ui.git
RUN cd rakam-ui && npm install

RUN git clone https://github.com/buremba/rakam.git
RUN cd rakam && mvn install -DskipTests && cd rakam/target && tar -zxvf *-bundle.tar.gz

RUN echo 'org.rakam=INFO\n\
io.netty=INFO' > log.properties

RUN env | grep RAKAM_ | awk  '{gsub(/\_/,".",$0); print substr(tolower($0), 8)}' > config.properties

RUN [ -s config.properties ] || apt-get update && apt-get install -y postgresql-9.4 postgresql-client-9.4 postgresql-contrib-9.4 \
								&& POSTGRES_PASSWORD=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1) \
								&& service postgresql start \
							    && su postgres -l -c "psql --command \"CREATE USER rakam WITH SUPERUSER PASSWORD 'dummy';\"" \
							    && su postgres -l -c 'createdb -O rakam rakam' \
							    # Rakam can automatically download & extract the database but we do this
							    # at compile time of the container because it increases the start time of the containers.
							    && wget -P /tmp http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz \
							    && gzip -d /tmp/GeoLite2-City.mmdb.gz \
							    && echo "store.adapter=postgresql\n\
store.adapter.postgresql.url=postgres://rakam:dummy@127.0.0.1:5432/rakam\n\
plugin.user.enabled=true\n\
plugin.user.mailbox.enable=true\n\
real-time.enabled=true\n\
event.stream.enabled=true\n\
event-explorer.enabled=true\n\
user.funnel-analysis.enabled=true\n\
user.retention-analysis.enabled=true\n\
plugin.geoip.enabled=true\n\
plugin.user.storage=postgresql\n\
http.server.address=0.0.0.0:9999\n\
plugin.user.storage.identifier_column=id\n\
plugin.user.mailbox.adapter=postgresql\n\
store.adapter.postgresql.max_connection=20\n\
plugin.geoip.database.url=file://tmp/GeoLite2-City.mmdb\n\
ui.custom-page.backend=jdbc\n\
ui.enable=true" > config.properties

WORKDIR /var/app/rakam

EXPOSE 9999

#-Dlog.enable-console=false
#-Dlog.output-file=../logs/app.log
ENTRYPOINT ([ -f /etc/init.d/postgresql ] && /etc/init.d/postgresql start); java -Dlog.levels-file=../log.properties -Dui.directory=../rakam-ui/app -cp $(echo rakam/target/rakam-*-bundle/rakam-*/lib)/*: org.rakam.ServiceStarter ../config.properties

RUN apt-get clean
