package com.bg.controller;

import com.bg.bean.TrafficUvCt;
import com.bg.service.TrafficStatsService;
import com.bg.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * @Package com.bg.controller.TrafficStatsController
 * @Author Chen.Run.ze
 * @Date 2025/4/17 11:19
 * @description: 流量域统计Controller
 */
@RestController
public class TrafficStatsController {
    @Autowired
    private TrafficStatsService trafficStatsService;

    @RequestMapping("/ch")
    public String getChUvCt(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficUvCt> trafficUvCtList = trafficStatsService.getChUvCt(date, limit);
        List chList = new ArrayList();
        List uvCtList = new ArrayList();
        for (TrafficUvCt trafficUvCt : trafficUvCtList) {
            chList.add(trafficUvCt.getCh());
            uvCtList.add(trafficUvCt.getUvCt());
        }


        return "{\"status\": 0,\"data\":{\"categories\": [\""+ StringUtils.join(chList,"\",\"")+"\"],\n" +
                "    \"series\": [{\"name\": \"渠道\",\"data\": ["+StringUtils.join(uvCtList,",")+"]}]}}";
    }
}
