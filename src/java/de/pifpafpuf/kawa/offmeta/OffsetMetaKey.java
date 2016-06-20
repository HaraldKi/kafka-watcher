package de.pifpafpuf.kawa.offmeta;

public class OffsetMetaKey extends MetaKey {
  public final short partition;
  public final String group;
  public final String topic;
  
  @Override
  public String getKey() {
    return group+','+topic+','+partition;
  }
  
  public OffsetMetaKey(short version, String group, String topic, short partition) {
    super(version);
    this.partition = partition;
    this.group = group;
    this.topic = topic;
  }

  public OffsetMsgValue decode(byte[] data, OffsetMsgValue vOld) {
    OffsetMsgValue vNew = OffsetMsgValue.decode(data, this);
    if (vNew==null) {
      if (vOld!=null) {
        vOld.expire();
      }
      return vOld;
    }
    return vNew;
  }
}
