package de.pifpafpuf.kawa.offmeta;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.serialization.Deserializer;

public enum OffsetsKeyDeserializer implements Deserializer<MetaKey> {
  INSTANCE;
  private static final short CURRENT_OFFSET_KEY_SCHEMA_VERSION = 1;
  private static final short CURRENT_GROUP_KEY_SCHEMA_VERSION = 2;
    

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
    return OffsetsKeyDeserializer.decode(data);
  }

  public static MetaKey decode(byte[] data) {
    ByteBuffer b = ByteBuffer.wrap(data);
    short version = b.getShort();
    if (version<=CURRENT_OFFSET_KEY_SCHEMA_VERSION) {
      String group = (String)Type.STRING.read(b);
      String topic = (String)Type.STRING.read(b);
      int partition = (Integer)Type.INT32.read(b);
      return new OffsetMetaKey(version, group, topic, (short)partition);
    } else if (version==CURRENT_GROUP_KEY_SCHEMA_VERSION)  {
      return new GroupMetaKey(version, (String)Type.STRING.read(b));
    } else {
      return new OffsetMetaKey(version, "invalid-version", "", (short)-1);
    }
  }

}
