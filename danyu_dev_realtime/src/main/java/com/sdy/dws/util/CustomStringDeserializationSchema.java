package com.sdy.dws.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @Package com.sdy.retail.v1.realtime.dws.util.CustomStringDeserializationSchema
 * @Author danyu-shi
 * @Date 2025/4/10 20:38
 * @description:
 */
public class CustomStringDeserializationSchema  implements DeserializationSchema<String> {
    @Override
    public String deserialize(byte[] bytes) throws IOException {
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
