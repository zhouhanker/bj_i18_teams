package com.lyx.stream.realtime.v2.app.constant;

/**
 * @Package com.lyx.retail.v1.realtime.constant.Constant
 * @Author yuxin_li
 * @Date 2025/4/7 22:09
 * @description: Constant
 */

public class Constant {
    public static final String KAFKA_BROKERS = "cdh01:9092,cdh02:9092,cdh03:9092";

    public static final String TOPIC_DB = "yuxin_li_db";
    public static final String TOPIC_LOG = "yuxin_li_log";

    //没更新所以连接不上mysql
    public static final String MYSQL_HOST = "10.160.60.17";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "Zh1028,./";
    public static final String HBASE_NAMESPACE = "ns_yuxin_li";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://10.160.60.17:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start_yuxin_li";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err_yuxin_li";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page_yuxin_li";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action_yuxin_li";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display_yuxin_li";
    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info_yuxin_li";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add_yuxin_li";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail_yuxin_li";
    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel_yuxin_li";
    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success_yuxin_li";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund_yuxin_li";
    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success_yuxin_li";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register_yuxin_li";


    //存疑：不一定对，有报错的时候再修改
    public static final String DORIS_FE_NODES = "10.160.60.14:8030";

    public static final String DORIS_DATABASE = "dev_yuxin_li";
}
