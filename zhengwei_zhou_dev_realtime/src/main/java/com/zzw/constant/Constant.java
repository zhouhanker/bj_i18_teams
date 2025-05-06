package com.zzw.constant;

/**
 * @Package com.zzw.retail.v1.realtime.constant.Constant
 * @Author zhengwei_zhou
 * @Date 2025/4/7 22:09
 * @description: Constant
 */

public class Constant {
    public static final String KAFKA_BROKERS = "cdh01:9092,cdh02:9092,cdh03:9092";

    public static final String TOPIC_DB = "zhengwei_zhou_db";
    public static final String TOPIC_LOG = "zhengwei_zhou_log";

    public static final String MYSQL_HOST = "10.160.60.17";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "Zh1028,./";
    public static final String HBASE_NAMESPACE = "ns_zhengwei_zhou";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://10.160.60.17:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start_zhengwei_zhou";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err_zhengwei_zhou";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page_zhengwei_zhou";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action_zhengwei_zhou";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display_zhengwei_zhou";
    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info_zhengwei_zhou";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add_zhengwei_zhou";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail_zhengwei_zhou";
    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel_zhengwei_zhou";
    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success_zhengwei_zhou";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund_zhengwei_zhou";
    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success_zhengwei_zhou";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register_zhengwei_zhou";


    public static final String DORIS_FE_NODES = "10.160.60.14:8030";

    public static final String DORIS_DATABASE = "dev_zhengwei_zhou";
}
//public static final String KAFKA_BROKERS = "cdh01:9092,cdh02:9092,cdh03:9092";
//
//    public static final String TOPIC_DB = "pengyu_zhu_db";
//    public static final String TOPIC_LOG = "pengyu_zhu_log";
//
//    public static final String MYSQL_HOST = "10.39.48.36";
//    public static final int MYSQL_PORT = 3306;
//    public static final String MYSQL_USER_NAME = "root";
//    public static final String MYSQL_PASSWORD = "Zh1028,./";
//    public static final String HBASE_NAMESPACE = "ns_pengyu_zhu";
//
//    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
//    public static final String MYSQL_URL = "jdbc:mysql://10.39.48.36:3306?useSSL=false";
//
//    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start_pengyu_zhu";
//    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err_pengyu_zhu";
//    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page_pengyu_zhu";
//    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action_pengyu_zhu";
//    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display_pengyu_zhu";
//    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info_pengyu_zhu";
//    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add_pengyu_zhu";
//    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail_pengyu_zhu";
//    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel_pengyu_zhu";
//    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success_pengyu_zhu";
//    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund_pengyu_zhu";
//    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success_pengyu_zhu";
//    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register_pengyu_zhu";
//
//
//    public static final String DORIS_FE_NODES = "10.39.48.33:8030";
//
//    public static final String DORIS_DATABASE = "dev_pengyu_zhu";
//}
