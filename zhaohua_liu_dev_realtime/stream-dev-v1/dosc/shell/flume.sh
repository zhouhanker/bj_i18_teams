sudo flume-ng agent \
--conf /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/etc/flume-ng/conf.empty/ \
--conf-file /opt/mock/flume/flume_file_kafka.conf \
--name a1 \
-Dflume.root.logger=INFO,console