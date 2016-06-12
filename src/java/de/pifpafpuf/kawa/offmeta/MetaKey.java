package de.pifpafpuf.kawa.offmeta;

import java.nio.ByteBuffer;

import org.apache.kafka.common.protocol.types.Type;

/**
 * see: https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/coordinator/GroupMetadataManager.scala
 */
public abstract class MetaKey {
  private static final short CURRENT_OFFSET_KEY_SCHEMA_VERSION = 1;
  private static final short CURRENT_GROUP_KEY_SCHEMA_VERSION = 2;
    
  public final short version;
  
  public MetaKey(short version) {
    this.version = version;
  }
  public abstract String getKey();
  
  @Override
  public final String toString() {
    String fullname = getClass().getName();
    int pos = fullname.lastIndexOf('.');
    String name = fullname.substring(pos+1);
    return name+"[version="+version+", key="+getKey()+"]";
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
  public abstract MsgValue decodeValue(byte[] value);

}
