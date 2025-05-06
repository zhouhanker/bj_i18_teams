package flink.realtime.dws.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @version 1.0
 * @Package flink.realtime.dws.test.Test01_Doris_SQL
 * @Author liu.bo
 * @Date 2025/5/4 14:42
 * @description: 演示Flink读写Doris_SQL
 */
public class Test01_Doris_SQL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000L);
//        tEnv.executeSql("CREATE TABLE flink_doris (\n" +
//                "    siteid INT,\n" +
//                "    citycode SMALLINT,\n" +
//                "    username STRING,\n" +
//                "    pv BIGINT\n" +
//                "    ) \n" +
//                "    WITH (\n" +
//                "      'connector' = 'doris',\n" +
//                "      'fenodes' = 'cdh01:8031',\n" +
//                "      'table.identifier' = 'test.table1',\n" +
//                "      'username' = 'root',\n" +
//                "      'password' = ''\n" +
//                ")");
        tEnv.executeSql("CREATE TABLE flink_doris (  " +
                "    siteid INT,  " +
                "    citycode INT,  " +
                "    username STRING,  " +
                "    pv BIGINT  " +
                ")WITH (" +
                "  'connector' = 'doris', " +
                "  'fenodes' = 'cdh01:8031', " +
                "  'table.identifier' = 'test.table1', " +
                "  'username' = 'root', " +
                "  'password' = '', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")  ");

        tEnv.executeSql("insert into flink_doris values(33, 3, '深圳', 3333)");
    }
}
