package lol.schroeder;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ScaleWeightEventDeserializationSchema implements DeserializationSchema<ScaleWeightEvent> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ScaleWeightEvent deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, ScaleWeightEvent.class);
    }

    @Override
    public boolean isEndOfStream(ScaleWeightEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ScaleWeightEvent> getProducedType() {
        return TypeInformation.of(ScaleWeightEvent.class);
    }
}
