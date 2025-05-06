package com.lxy.realtime.function;

import com.lxy.realtime.bean.TradeProvinceOrderBean;
import com.lxy.realtime.utils.DateFormatUtil;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.lxy.realtime.function.TradeProvinceOrderWindowFunction
 * @Author xinyu.luo
 * @Date 2025/5/5 10:35
 * @description:
 */
public class TradeProvinceOrderWindowFunction implements WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) {
        TradeProvinceOrderBean orderBean = input.iterator().next();
        long startTs = window.getStart() / 1000;
        long endTs = window.getEnd() / 1000;
        String stt = DateFormatUtil.tsToDateTime(startTs);
        String edt = DateFormatUtil.tsToDateTime(endTs);
        String curDate = DateFormatUtil.tsToDate(startTs);
        orderBean.setStt(stt);
        orderBean.setEdt(edt);
        orderBean.setCurDate(curDate);
        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
        out.collect(orderBean);
    }
}
