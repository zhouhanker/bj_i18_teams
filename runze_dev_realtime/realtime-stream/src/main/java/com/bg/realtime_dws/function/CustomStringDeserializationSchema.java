package com.bg.realtime_dws.function;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * @Package com.jiao.dws.function.CustomStringDeserializationSchema
 * @Author Chen.Run.ze
 * @Date 2025/4/17 8:35
 * @description:
 */
public class CustomStringDeserializationSchema  implements DeserializationSchema<String> {
    @Override
    public String deserialize(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return new String(bytes);
    }

    @Override
    public boolean isEndOfStream(String s) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
