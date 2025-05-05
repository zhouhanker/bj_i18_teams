package com.bg.controller;

import com.bg.bean.TradeProvinceOrderAmount;
import com.bg.service.TradeStatsService;
import com.bg.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Package com.bg.controller.TradeStatsController
 * @Author Chen.Run.ze
 * @Date 2025/4/17 10:49
 * @description: 交易域统计Controller
 */
@RestController
public class TradeStatsController {
    @Autowired
    private TradeStatsService tradeStatsService;

    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            //说明请求的时候，没有传递日期参数，将当天日期作为查询的日期
            date = DateFormatUtil.now();
        }
        BigDecimal gmv = tradeStatsService.getGMV(date);
        return "{\n" +
                "\t\t  \"status\": 0,\n" +
                "\t\t  \"data\": "+gmv+"\n" +
                "\t\t}";
    }

    @RequestMapping("/province")
    public String getProvinceAmount(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = DateFormatUtil.now();
        }
        List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStatsService.getProvinceAmount(date);

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceOrderAmountList.size(); i++) {
            TradeProvinceOrderAmount provinceOrderAmount = provinceOrderAmountList.get(i);
            jsonB.append("{\"name\": \"").append(provinceOrderAmount.getProvinceName()).append("\",\"value\": ").append(provinceOrderAmount.getOrderAmount()).append("}");
            if(i < provinceOrderAmountList.size() - 1){
                jsonB.append(",");
            }
        }

        jsonB.append("],\"valueName\": \"交易额\"}}");
        return jsonB.toString();
    }
}
