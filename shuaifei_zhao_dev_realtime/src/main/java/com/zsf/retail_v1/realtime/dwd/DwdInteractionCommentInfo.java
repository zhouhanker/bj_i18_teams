package com.zsf.retail_v1.realtime.dwd;


import com.zsf.retail_v1.realtime.constant.Constant;
import com.zsf.retail_v1.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.zsf.retail.v1.realtime.dwd.DwdInteractionCommentInfo
 * @Author zhao.shuai.fei
 * @Date 2025/4/9 21:51
 * @description: 评论事实表
 */
public class DwdInteractionCommentInfo {
    public static void main(String[] args) throws Exception {
        //流处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度，
        env.setParallelism(4);
        // flink sql初始化
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从kafka的topic_db主题中读取数据 创建动态表       ---kafka连接器
        tableEnv.executeSql("create table topic_db(\n" +
                "    `after` map<string,string>,\n" +
                "    `source` map<string,string>,\n" +
                "    `op` string,\n" +
                "    `ts_ms` BIGINT,\n" +
                "    proc_time as proctime()\n" +
                ")WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//        tableEnv.executeSql("select * from topic_db").print();

//        readOdsDb(tableEnv,"dwd_interaction_comment_info");

        Table commentInfo = tableEnv.sqlQuery("select \n" +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['sku_id'] sku_id,\n" +
                "`after`['appraise'] appraise,\n" +
                "`after`['comment_txt'] comment_txt,\n" +
                "`source`['ts_ms'] ts_ms,\n" +
                "`proc_time` \n" +
                "from topic_db where `source`['table']='comment_info'");
//        commentInfo.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("comment_info",commentInfo);

        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'sx_002:dim_base_dic',\n" +
                " 'zookeeper.quorum' = 'cdh01:2181,cdh02:2181,cdh03:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'partial',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")");

//        tableEnv.executeSql("select * from base_dic").print();


        //TODO 将评论表和字典表进行关联                        --- lookup Join
        Table joinedTable = tableEnv.sqlQuery("SELECT\n" +
                "    id,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    appraise,\n" +
                "    dic.dic_name appraise_name,\n" +
                "    comment_txt,\n" +
                "    ts_ms\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic\n" +
                "    ON c.appraise = dic.dic_code");
//        joinedTable.execute().print();

        //TODO 将关联的结果写到kafka主题中                    ---upsert kafka连接器
        //创建动态表和要写入的主题进行映射

        tableEnv.executeSql("CREATE TABLE "+ Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    appraise string,\n" +
                "    appraise_name string,\n" +
                "    comment_txt string,\n" +
                "    ts string,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);


//        env.execute("dwd02");
    }
}
