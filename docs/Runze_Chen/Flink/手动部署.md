1.	将realtime-dev父模块install到本地仓库
2.	将realtime-common模块install到本地仓库
3.	打包realtime-stream模块,上传到flink目录下
4.  启动历史服务mapred --daemon start historyserver
5.  提交DwdTradeOrderDetail、DwsTradeProvinceOrderWindow应用
6.  将common本地仓库的jar包上传的FLink的lib目录下
# 应用模式提交任务 本地提交
./flink run-application \
-d \
-t yarn-application \
-ynm DbusCdc2DimHbase \
-yjm 900 \
-ytm 900 \
-yqu root.default \
-c com.bg.FlinkCDC /opt/soft/flink-1.17.1/realtime-stream-1.0-SNAPSHOT.jar


./flink run-application \
-t yarn-application \
-d \
-Djobmanager.memory.process.size=1024mb \
-Dtaskmanager.memory.process.size=1024mb \
-Dtaskmanager.numberOfTaskSlots=2 \
-c com.bg.FlinkCDC /opt/soft/flink-1.17.1/realtime-stream-1.0-SNAPSHOT.jar
