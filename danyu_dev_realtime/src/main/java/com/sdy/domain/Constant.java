package com.sdy.domain;

/**
 * @Package com.sdy.common.domain.Constant
 * @Author danyu-shi
 * @Date 2025/4/8 19:56
 * @description:
 */
public class Constant {
    public static final String KAFKA_BROKERS = "cdh01:9092,cdh02:9092,cdh03:9092";

    public static final String TOPIC_DB = "stream-dev2-danyushi";
    public static final String TOPIC_LOG = "stream-dev1-danyushi";

    public static final String MYSQL_HOST = "10.160.60.17";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "Zh1028,./";
    public static final String HBASE_NAMESPACE = "ns_danyu_shi";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://10.160.60.17:3306?useSSL=false";

//    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
//    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
//    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
//    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
//    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
//
//    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
//    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
//
//    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
//
//    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";
//
//    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
//    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
//
//    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";
//
//    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
//
    public static final String DORIS_FE_NODES = "hadoop102:7030";
//
    public static final String DORIS_DATABASE = "dev_danyu";
}
