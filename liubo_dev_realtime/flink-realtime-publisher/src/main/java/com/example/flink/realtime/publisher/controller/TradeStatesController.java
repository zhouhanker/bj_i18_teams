package com.example.flink.realtime.publisher.controller;

import com.example.flink.realtime.publisher.bean.TradeProvinceOrderAmount;
import com.example.flink.realtime.publisher.service.TradeStateService;
import com.example.flink.realtime.publisher.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

/**
 * @ version 1.0
 * @ Package com.example.flink.realtime.publisher.controller.TradeStatesController
 * @ Author liu.bo
 * @ Date 2025/5/4 14:57
 * @ description:交易域统计Controller
 */

@RestController
public class TradeStatesController {
    @Autowired
    private TradeStateService tradeStateService;

    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            //说明请求的时候，没有传递日期参数，将当天日期作为查询的日期
            date = DateFormatUtil.now();
        }
        BigDecimal gmv = tradeStateService.getGMV(date);
        return "{\n" +
                "\"status\":0,\n" +
                "\"data\":" + gmv + "\n" +
                "}";
    }

    @RequestMapping("/province")
    public String getProvinceAmount(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = DateFormatUtil.now();
        }
        List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStateService.getProvinceAmount(date);

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceOrderAmountList.size(); i++) {
            TradeProvinceOrderAmount provinceOrderAmount = provinceOrderAmountList.get(i);
            jsonB.append("{\"name\": \""+provinceOrderAmount.getProvinceName()+"\",\"value\": "+provinceOrderAmount.getOrderAmount()+"}");
            if(i < provinceOrderAmountList.size() - 1){
                jsonB.append(",");
            }
        }
        jsonB.append("],\"valueName\": \"交易额\"}}");
        return jsonB.toString();
    }
}

