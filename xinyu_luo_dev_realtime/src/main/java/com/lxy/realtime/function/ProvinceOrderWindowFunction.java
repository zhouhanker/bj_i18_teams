package com.lxy.realtime.function;

import com.alibaba.fastjson.JSONObject;
import com.lxy.realtime.bean.TradeProvinceOrderBean;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;

/**
 * @Package com.lxy.realtime.function.ProvinceOrderWindowFunction
 * @Author xinyu.luo
 * @Date 2025/5/5 10:29
 * @description: ProvinceOrderWindowFunction
 */
public class ProvinceOrderWindowFunction implements MapFunction<JSONObject, TradeProvinceOrderBean> {
    @Override
    public TradeProvinceOrderBean map(JSONObject jsonObj) {
        String provinceId = jsonObj.getString("province_id");
        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
        Long ts = jsonObj.getLong("ts_ms");
        String orderId = jsonObj.getString("order_id");
        return TradeProvinceOrderBean.builder()
                .provinceId(provinceId)
                .orderAmount(splitTotalAmount)
                .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                .ts_ms(ts)
                .build();
    }
}
