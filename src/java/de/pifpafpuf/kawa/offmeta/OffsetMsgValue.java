package de.pifpafpuf.kawa.offmeta;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.kafka.common.protocol.types.Type;

public class OffsetMsgValue extends MsgValue {
  public final short version;
  public final String metadata;
  public final long offset;
  public final long commitStamp;
  public final long expiresStamp;
  public final OffsetMetaKey key;
  private long expired = -1;    // no tyet expired if <0
  private long head = -1;
  
  public OffsetMsgValue(short version, String metadata, long offset,
                        long timestamp, long expiresStamp,
                        OffsetMetaKey key) {
    this.version = version;
    this.metadata = metadata;
    this.offset = offset;
    this.commitStamp = timestamp;
    this.expiresStamp = expiresStamp;
    this.key = key;
  }

  public void expire() {
    expired = System.currentTimeMillis();
  }
  public long getExpired() {
    return expired;
  }
  public boolean isExpired() {
    return expired>=0;
  }
  public void setHead(long offset) {
    this.head = offset;
  }
  public long getHead() {
    return head;
  }

  public static OffsetMsgValue decode(byte[] data, OffsetMetaKey key) {
    if (data==null) {
      return null;
    }
    short version = -1;
    String metadata = "";
    long offset = -1;
    long timestamp = -1;
    long expiresStamp = -1;

    ByteBuffer b = ByteBuffer.wrap(data);
    version = b.getShort();
    if (version>=0 && version<=1) {
      offset = (long)Type.INT64.read(b);
      metadata = (String)Type.STRING.read(b);
      timestamp = (long)Type.INT64.read(b);
    }
    if (version==1) {
      expiresStamp = (long)Type.INT64.read(b);
    } else {
      metadata = "wrong version information "+version+" ignored";
    }

    return new OffsetMsgValue(version, metadata,
                              offset, timestamp, expiresStamp,
                              key);
  }

  @Override
  public String toString() {
    DateFormat df = new SimpleDateFormat("yyyyMMdd'_'HH:mm:ss");
    return "OffsetMsgValue[version="+version+", metadata="+metadata
        +", offset="+offset+", commitStamp="+df.format(commitStamp)
        +", expiresStamp="+df.format(expiresStamp)+"]";
  }

}
