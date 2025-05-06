package com.example.flink.realtime.publisher.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

/**
 * @version 1.0
 * @Package com.example.flink.realtime.publisher.util.DateFormatUtil
 * @Author liu.bo
 * @Date 2025/5/4 15:02
 * @description:
 */
public class DateFormatUtil {
    public static Integer now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
