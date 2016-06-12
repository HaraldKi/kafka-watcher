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


  public OffsetMsgValue(short version, String metadata, long offset,
                        long timestamp, long expiresStamp) {
    this.version = version;
    this.metadata = metadata;
    this.offset = offset;
    this.commitStamp = timestamp;
    this.expiresStamp = expiresStamp;
  }

  public static OffsetMsgValue decode(byte[] data) {
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
                              offset, timestamp, expiresStamp);
  }

  @Override
  public String toString() {
    DateFormat df = new SimpleDateFormat("yyyyMMdd'_'HH:mm:ss");
    return "OffsetMsgValue[version="+version+", metadata="+metadata
        +", offset="+offset+", commitStamp="+df.format(commitStamp)
        +", expiresStamp="+df.format(expiresStamp)+"]";
  }

}
