package dwd;


import constant.Constant;
import util.SQLUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.cm.dwd.DwdInteractionCommentInfo
 * @Author chen.ming
 * @Date 2025/4/11 10:32
 * @description: 用户评论表
 */
public class DwdInteractionCommentInfo{
    @SneakyThrows
    public static void main(String[] args)  {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
//
        tenv.executeSql("" +
                "CREATE TABLE db (\n" +
                "  after Map<String,String>,\n" +
                "  source  Map<String,String>,\n" +
                "  op  String,\n" +
                "  ts_ms  bigint,\n" +
                "  before MAP<string,string> ,\n" +
                "  proc_time  AS proctime()\n "+
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'chenming_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//

//        Table table = tenv.sqlQuery("select * from db ");
//        tenv.toChangelogStream(table).print();
// TODO 关联数据 过滤 评论数据
         Table table1 = tenv.sqlQuery("select " +
                "after['id'] as id," +
                "after['user_id'] as user_id," +
                "after['sku_id'] as sku_id," +
                "after['appraise'] as appraise," +
                "after['comment_txt'] as comment_txt," +
                "ts_ms as ts," +
                "proc_time " + //添加 proc_time 字段用于后续窗口计算或事件时间处理；
                "from db where source['table'] = 'comment_info'");
//        tenv.toChangelogStream(table1).print("过滤===>");
//通过SQL查询过滤出source['table']为comment_info的消息，并提取其中的相关字段。然后将结果注册为一个临时视图comment_info
        tenv.createTemporaryView("comment_info",table1);
// TODO 关联数据
        tenv.executeSql("CREATE TABLE hbase (\n" +
                " dic_code String,\n" + // 字典码
                " info ROW<dic_name String>,\n" + // 包含字典名称的信息行
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" + //主键为dic_code，但不强制执行唯一性约束
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'ns_chenming:dim_base_dic',\n" +
                " 'zookeeper.quorum' = 'cdh01,cdh02,cdh03:2181'\n" +
                ");");

//        Table table2 = tenv.sqlQuery("select * from hbase");
//        tenv.toChangelogStream(table2).print();
//
        Table table3 = tenv.sqlQuery("SELECT  " +
                " id," +
                "user_id," +
                "sku_id," +
                "appraise," +
                "dic.dic_name as appraise_name," + //评分名称
                "comment_txt," +
                "ts \n" +
                "FROM comment_info AS c \n" +
                "  left join hbase as dic \n" +
                "  ON c.appraise = dic.dic_code;");
        table3.execute().print();
//

        tenv.executeSql("CREATE TABLE "+ Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    appraise string,\n" + // 评分
                "    appraise_name string,\n" + // 评分名称
                "    comment_txt string,\n" + // 评论内容为
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
//         写入
        table3.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
        Table table4 = tenv.sqlQuery("select * from   dwd_interaction_comment_info_chenming ");
        tenv.toChangelogStream(table4).print();
        env.execute();
    }
}
