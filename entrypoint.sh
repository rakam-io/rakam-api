#!/bin/sh

set -x
echo $@
if [ $1 -e 'custom' ]
   java -Dstore.adapter=postgresql -Dplugin.user.enabled=true -Devent-explorer.enabled=true -Dcustom-data-source.enabled=true -Duser.funnel-analysis.enabled=true -Dplugin.user.enable-user=true -cp /app/rakam-*/lib/\*.jar org.rakam.ServiceStarter
then
   java -Dstore.adapter=postgresql -Dplugin.user.enabled=true -Devent-explorer.enabled=true -Dcustom-data-source.enabled=true -Duser.funnel-analysis.enabled=true -Dplugin.user.enable-user-mapping=true -Duser.retention-analysis.enabled=true -Dplugin.geoip.enabled=true -Dplugin.user.storage=postgresql -Dhttp.server.address=0.0.0.0:9999 -Dplugin.user.storage.identifier-column=id -Dplugin.geoip.database.url=file://tmp/GeoLite2-City.mmdb -cp /app/rakam-*/lib/\*.jar org.rakam.ServiceStarter
pwd
fi

