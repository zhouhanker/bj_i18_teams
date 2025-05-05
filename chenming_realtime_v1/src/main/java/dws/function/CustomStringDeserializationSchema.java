package dws.function;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @Package com.cm.dws.function.CustomStringDeserializationSchema
 * @Author chen.ming
 * @Date 2025/4/17 8:35
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
