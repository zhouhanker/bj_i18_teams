package realtime.dwd.db.app;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.common.base.BaseApp;
import realtime.common.base.BaseSQLApp;
import realtime.common.constant.Constant;
import realtime.common.util.SQLutil;

/**
 * @Package realtime.dwd.db.app.DwdInteractionComment
 * @Author zhaohua.liu
 * @Date 2025/4/14.18:20
 * @description:
 */
public class DwdInteractionComment extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwdInteractionComment().start(20004,4, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
    @Override
    public void handle(StreamTableEnvironment tEnv) {
        //从kafka的topic_db主题中读取数据 创建动态表
        readOdsDb(tEnv, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

//        过滤出评论数据
        Table commentInfoTable = tEnv.sqlQuery(
                "select " +
                        " `after`['id'] id, " +
                        " `after`['user_id'] user_id, " +
                        " `after`['sku_id'] sku_id, " +
                        " `after`['appraise'] appraise, " +
                        " `after`['comment_txt'] comment_txt," +
                        " ts_ms," +
                        " pt " +
                        "from ods_initial " +
                        "where `source`['table']='comment_info'"
        );
//        commentInfoTable.execute().print();
        //将表对象注册到表执行环境中
        tEnv.createTemporaryView("comment_info",commentInfoTable);
        //从HBase中读取字典数据 创建动态表base_dic
        readHbaseDic(tEnv);
//        时态表连接特性,使用comment_info表的pt字段,也就是处理时间,去关联base_dic中的数据,确保读取的时间版本正确
        // Lookup Join:   t1 join t2 for system_time as of t1.pt on t1.id = t2.id
        //通常是将一个实时的数据流与一个外部存储（如数据库、缓存等）中的表进行连接
        //不需要一次性加载和处理大量数据，而是按需从外部存储中查询数据
        Table joinTable = tEnv.sqlQuery(
                "select " +
                        "id," +
                        "user_id," +
                        "sku_id," +
                        "appraise," +
                        "dic.dic_name as appraise_name," +
                        "comment_txt," +
                        "ts_ms " +
                        "from comment_info as c " +
                        "join base_dic for system_time as of c.pt as dic " +
                        " on c.appraise = dic.dic_code"
        );
//        joinTable.execute().print();
        //创建upsert_kafka表,映射kafka中的数据建表
        tEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+"(" +
                        "id string," +
                        "user_id string," +
                        "sku_id string," +
                        "appraise string," +
                        "appraise_name string," +
                        "comment_txt string," +
                        "ts_ms bigint ," +
                        "primary key (id) not enforced" +
                        ")"+ SQLutil.getUpsertKafkaDDl(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO)
        );
        //数据写入kafka
        joinTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);


    }
}
