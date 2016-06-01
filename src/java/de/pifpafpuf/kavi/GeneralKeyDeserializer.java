package de.pifpafpuf.kavi;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import de.pifpafpuf.kavi.offmeta.OffsetsKeyDeserializer;

public enum GeneralKeyDeserializer implements Deserializer<Object> {
  KEY {
    @Override
    public Object deserialize(String topic, byte[] data) {
      if (QueueWatcher.TOPIC_OFFSET.equals(topic)) {
        return OffsetsKeyDeserializer.INSTANCE.deserialize(topic, data);
      }
      return sds.deserialize(topic, data);
    }
  };

  private static final StringDeserializer sds = new StringDeserializer();

  @Override
  public void close() {
    // nothing to close
  }

  @Override
  public void configure(Map<String,?> arg0, boolean arg1) {
    // nothing to configure
  }


}
