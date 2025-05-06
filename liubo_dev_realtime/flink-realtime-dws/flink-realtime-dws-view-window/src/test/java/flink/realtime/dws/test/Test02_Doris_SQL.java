package flink.realtime.dws.test;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @version 1.0
 * @Package flink.realtime.dws.test.Test02_Doris_SQL
 * @Author liu.bo
 * @Date 2025/5/4 14:43
 * @description:  API操作doris
 */
public class Test02_Doris_SQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L);

//        DorisOptions.Builder builder = DorisOptions.builder()
//                .setFenodes("cdh01:8031")
//                .setTableIdentifier("test.table1")
//                .setUsername("root")
//                .setPassword("");
//
//        DorisSource<List<?>> dorisSource = DorisSource.<List<?>>builder()
//                .setDorisOptions(builder.build())
//                .setDorisReadOptions(DorisReadOptions.builder().build())
//                .setDeserializer(new SimpleListDeserializationSchema())
//                .build();
//
//        DataStreamSource<List<?>> stream1 = env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris source");
//        stream1.print();
//        env.execute();
        DataStreamSource<String> source = env
                .fromElements("{\"siteid\": \"550\", \"citycode\": \"1001\", \"username\": \"ww\",\"pv\": \"100\"}");
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes("cdh01:8031")
                        .setTableIdentifier("test.table1")
                        .setUsername("root")
                        .setPassword("")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setBufferCount(3) // 批次条数: 默认 3
                        .setBufferSize(8 * 1024) // 批次大小: 默认 1M
                        .setCheckInterval(3000) // 批次输出间隔   三个对批次的限制是或的关系
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
        source.sinkTo((Sink<String, ?, ?, ?>) sink);
        env.execute();
    }
}
