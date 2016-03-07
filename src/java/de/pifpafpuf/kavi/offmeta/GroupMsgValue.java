package de.pifpafpuf.kavi.offmeta;

import java.nio.ByteBuffer;

import org.apache.kafka.common.protocol.types.Type;

public class GroupMsgValue extends MsgValue {
  // only partially implemented
  public final short version;
  public final String protocol;
  
  public GroupMsgValue(short version, String protocol) {
    this.version = version;
    this.protocol = protocol;
  }
  
  public static GroupMsgValue decode(byte[] data) {    if (data==null) {
      return new GroupMsgValue((short)-1, "no data");
    }

    ByteBuffer b = ByteBuffer.wrap(data);
    short version = b.getShort();
    if (version==0) {
      String protocol = (String)Type.STRING.read(b);
      return new GroupMsgValue(version, protocol); 
    }
    
    return new GroupMsgValue(version, "wrong version");
  }

  @Override
  public String toString() {
    return "GroupMsgValue[version="+version+", protocol="+protocol+"]";
  }
  
  
}
