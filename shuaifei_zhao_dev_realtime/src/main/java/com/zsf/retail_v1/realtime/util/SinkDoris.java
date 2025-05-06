package com.zsf.retail_v1.realtime.util;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

import java.util.Properties;

/**
 * @Package com.hwq.until.SinkDoris
 * @Author hu.wen.qi
 * @Date 2025/4/16 22:15
 * @description: 1
 */
public class SinkDoris {
        public static DorisSink<String> getDorisSink(String db,String tableName){
            Properties props = new Properties();
            props.setProperty("format", "json");
            props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

            DorisSink<String> sink = DorisSink.<String>builder()
                    .setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                            .setFenodes("cdh03:8030")
                            .setTableIdentifier( db+ "." + tableName)
                            .setUsername("root")
                            .setPassword("root")
                            .build())
                    .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
//.setLabelPrefix("doris-label") // stream-load 导入的时候的 label 前缀
                            .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                            .setDeletable(false)
                            .setBufferCount(3) // 用于缓存stream load数据的缓冲条数: 默认 3
                            .setBufferSize(1024*1024) //用于缓存stream load数据的缓冲区大小: 默认 1M
                            .setMaxRetries(3)
                            .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                            .build())
                    .setSerializer(new SimpleStringSerializer())
                    .build();
            return sink;
    }
}
