package de.pifpafpuf.kawa.offmeta;

import java.nio.ByteBuffer;

import org.apache.kafka.common.protocol.types.Type;

public class GroupMsgValue extends MsgValue {
  // only partially implemented
  public final short version;
  public final String protocol;
  public final GroupMetaKey key;
  private long expired = -1;
  
  public GroupMsgValue(short version, String protocol, GroupMetaKey key) {
    this.version = version;
    this.protocol = protocol;
    this.key = key;
  }  
  /*+******************************************************************/
  public void expire() {
    this.expired = System.currentTimeMillis();
  }
  /*+******************************************************************/
  public boolean isExpired() {
    return expired>=0;
  }
  /*+******************************************************************/
  public static GroupMsgValue decode(byte[] data, GroupMetaKey key) {  
    if (data==null) {
      return null;
    }

    ByteBuffer b = ByteBuffer.wrap(data);
    short version = b.getShort();
    if (version==0) {
      String protocol = (String)Type.STRING.read(b);
      return new GroupMsgValue(version, protocol, key); 
    }
    
    return new GroupMsgValue(version, "wrong version", key);
  }

  @Override
  public String toString() {
    return "GroupMsgValue[version="+version+", protocol="+protocol+"]";
  }
  
  
}
