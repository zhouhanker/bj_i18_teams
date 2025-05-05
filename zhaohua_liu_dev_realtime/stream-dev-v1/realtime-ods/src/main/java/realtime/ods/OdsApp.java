package realtime.ods;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import realtime.common.base.BaseApp;
import realtime.common.constant.Constant;

/**
 * @Package realtime.ods.cdcBinlog
 * @Author zhaohua.liu
 * @Date 2025/4/9.15:28
 * @description: cdc采集mysql的业务数据
 */
public class OdsApp extends BaseApp {
    //todo 全量读取mysql数据,并发送到kafka
    public static void main(String[] args) throws Exception {
        new OdsApp().start(20001,4, Constant.TOPIC_ODS_INITIAL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStreamDS) {

    }
}
