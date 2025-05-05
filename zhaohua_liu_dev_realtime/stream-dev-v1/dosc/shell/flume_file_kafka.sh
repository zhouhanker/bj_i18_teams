#!/bin/sh
flumeConfDir="/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/etc/flume-ng/conf.empty/"
flumeConfFile="/opt/mock/flume/flume_file_kafka.conf"
positionFile="/opt/mock/flume/position.json"
logFile="/opt/mock/log/app.*"


# 初始化本地断点续传文件
rm -rf $positionFile
echo "[]" > $positionFile
chmod 777 $positionFile

# 创建flume配置文件
rm -rf $flumeConfFile
cat > $flumeConfFile <<EOF
a1.sources = r1
a1.channels = c1

a1.sources.r1.channels = c1

a1.sources.r1.type = TAILDIR
a1.sources.r1.positionFile = $positionFile
a1.sources.r1.filegroups = f1
a1.sources.r1.filegroups.f1 = $logFile


a1.channels.c1.type = org.apache.flume.channel.kafka.KafkaChannel
a1.channels.c1.kafka.bootstrap.servers = cdh01:9092,cdh02:9092,cdh03:9092
a1.channels.c1.kafka.topic = topic_log
a1.channels.c1.kafka.consumer.group.id = flume_consumer
a1.channels.c1.parseAsFlumeEvent = false

EOF
chmod 777 $flumeConfFile


sudo flume-ng agent \
--conf $flumeConfDir \
--conf-file $flumeConfFile \
--name a1 \
-Dflume.root.logger=INFO,console
