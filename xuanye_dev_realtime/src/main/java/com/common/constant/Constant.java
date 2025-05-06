package com.common.constant;

public class Constant {
    // Kafka 集群的 brokers 地址，多个地址用逗号分隔
    public static final String KAFKA_BROKERS = "cdh01:9092,cdh02:9092,cdh03:9092";
    // 用于存储数据库相关数据的 Kafka 主题
    public static final String TOPIC_DB = "xuanye_chang_Business";
    // 用于存储日志数据的 Kafka 主题
    public static final String TOPIC_LOG = "xuanye_chang_log";

    // MySQL 数据库主机地址
    public static final String MYSQL_HOST = "cdh01";
    // MySQL 数据库端口
    public static final int MYSQL_PORT = 3306;
    // MySQL 数据库用户名
    public static final String MYSQL_USER_NAME = "root";
    // MySQL 数据库密码
    public static final String MYSQL_PASSWORD = "Zh1028,./";
    // HBase 命名空间
    public static final String HBASE_NAMESPACE = "xuanye_chang";

    // MySQL JDBC 驱动类
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    // MySQL 数据库连接 URL
    public static final String MYSQL_URL = "jdbc:mysql://10.160.60.17:3306?useSSL=false";

    // 用于存储启动流量相关数据的 Kafka 主题
    public static final String TOPIC_DWD_TRAFFIC_START = "xuanye_dwd_traffic_start";
    // 用于存储流量错误相关数据的 Kafka 主题
    public static final String TOPIC_DWD_TRAFFIC_ERR = "xuanye_dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "xuanye_dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "xuanye_dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "xuanye_dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "xuanye_chang_dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "xuanye_chang_dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "xuanye_dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "xuanye_dwd_trade_order_cancel";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "xuanye_dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "xuanye_dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "xuanye_dwd_trade_refund_payment_success";

    public static final String TOPIC_DWD_USER_REGISTER = "xuanye_dwd_user_register";
    // HBase 连接的 Zookeeper 地址，假设 HBase 使用 Zookeeper 进行管理
    public static final String HBASE_ZK_QUORUM = "hadoop102,hadoop103,hadoop104";
    // HBase Zookeeper 客户端端口
    public static final int HBASE_ZK_CLIENT_PORT = 2181;
    public static final String DORIS_DATABASE = "realtime_v1_xuanye_chang";
    public static final String DORIS_FE_NODES = "10.39.48.33:8030";


}
