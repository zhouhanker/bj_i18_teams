package com.struggle.flink.realtime.common.util;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Base64;


/**
 * @ version 1.0
 * @ Package com.struggle.flink.realtime.common.util.decodeBase64
 * @ Author liu.bo
 * @ Date 2025/5/3 14:22
 * @ description:
 */
public class decodeBase64 {
    // Base64 解码工具方法
    public static BigDecimal decodeBase64ToBigDecimal(String base64Str) {
        if (base64Str == null || base64Str.isEmpty()) {
            return BigDecimal.ZERO; // 默认值，可根据业务调整
        }
        try {
            byte[] decodedBytes = Base64.getDecoder().decode(base64Str);
            String decodedStr = new String(decodedBytes, StandardCharsets.UTF_8);
            return new BigDecimal(decodedStr);
        } catch (Exception e) {
            // 解码失败时返回默认值或抛出异常（根据业务需求）
            return BigDecimal.ZERO;
        }
    }
}
