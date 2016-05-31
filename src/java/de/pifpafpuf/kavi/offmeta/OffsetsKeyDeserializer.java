package de.pifpafpuf.kavi.offmeta;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public enum OffsetsKeyDeserializer implements Deserializer<MetaKey> {
  INSTANCE;

  @Override
  public void close() {
    // nothing to close
  }

  @Override
  public void configure(Map<String,?> arg0, boolean arg1) {
    // nothing to configure
  }

  @Override
  public MetaKey deserialize(String topic, byte[] data) {
    return MetaKey.decode(data);
  }

}
