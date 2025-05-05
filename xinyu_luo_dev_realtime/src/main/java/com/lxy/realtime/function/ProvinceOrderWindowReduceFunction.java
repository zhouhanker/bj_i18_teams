package com.lxy.realtime.function;

import com.lxy.realtime.bean.TradeProvinceOrderBean;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @Package com.lxy.realtime.function.ProvinceOrderWindowReduceFunction
 * @Author xinyu.luo
 * @Date 2025/5/5 10:32
 * @description: ProvinceOrderWindowReduceFunction
 */
public class ProvinceOrderWindowReduceFunction implements ReduceFunction<TradeProvinceOrderBean> {
    @Override
    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) {
        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
        return value1;
    }
}
