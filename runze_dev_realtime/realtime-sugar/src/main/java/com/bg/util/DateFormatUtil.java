package com.bg.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

/**
 * @Package com.bg.util.DateFormatUtil
 * @Author Chen.Run.ze
 * @Date 2025/4/17 10:52
 * @description: 日期util
 */
public class DateFormatUtil {
    public static Integer now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
