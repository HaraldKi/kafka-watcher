package de.pifpafpuf.kavi.offmeta;

public class OffsetInfo {
  public final OffsetMetaKey key;
  public final OffsetMsgValue value;
  
  public OffsetInfo(OffsetMetaKey key, OffsetMsgValue value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public String toString() {
    return "OffsetInfo[key="+key+", value="+value+"]";
  }
  
  
}
