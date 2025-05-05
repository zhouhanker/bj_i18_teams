package com.lxy.realtime.function;


import com.lxy.realtime.bean.CartAddUuBean;
import com.lxy.realtime.utils.DateFormatUtil;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.lxy.realtime.function.CartAddUuWindowFunction
 * @Author xinyu.luo
 * @Date 2025/5/4 21:33
 * @description: CartAddUuWindowFunction
 */
public class CartAddUuWindowFunction implements AllWindowFunction<Long, CartAddUuBean, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) {
        Long cartUUCt = values.iterator().next();
        long startTs = window.getStart() / 1000;
        long endTs = window.getEnd() / 1000;
        String stt = DateFormatUtil.tsToDateTime(startTs);
        String edt = DateFormatUtil.tsToDateTime(endTs);
        String curDate = DateFormatUtil.tsToDate(startTs);
        out.collect(new CartAddUuBean(
                stt,
                edt,
                curDate,
                cartUUCt
        ));
    }
}
