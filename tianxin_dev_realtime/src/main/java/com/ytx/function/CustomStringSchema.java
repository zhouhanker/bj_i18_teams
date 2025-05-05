package com.ytx.function;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class CustomStringSchema implements DeserializationSchema<String> {
    @Override
    public String deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        return new String(message);
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeExtractor.getForClass(String.class);
    }
}
