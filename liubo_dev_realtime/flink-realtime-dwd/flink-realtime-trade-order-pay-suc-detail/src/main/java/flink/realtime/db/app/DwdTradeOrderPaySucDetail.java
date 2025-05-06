package flink.realtime.db.app;

import com.struggle.flink.realtime.common.base.BaseSQLApp;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @version 1.0
 * @Package flink.realtime.db.app.DwdTradeOrderPaySucDetail
 * @Author liu.bo
 * @Date 2025/5/4 14:12
 * @description:  交易退款支付成功事实表
 */
public class DwdTradeOrderPaySucDetail  extends BaseSQLApp {

//        public static void main(String[] args) {
//        new DwdTradeOrderPaySucDetail().start(
//                10016,
//                4,
//                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS
//        );
//    }
    @Override
    public void handle(StreamTableEnvironment tableEnv) {

    }
}
