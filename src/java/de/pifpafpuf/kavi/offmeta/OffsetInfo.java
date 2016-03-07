package de.pifpafpuf.kavi.offmeta;

public class OffsetInfo {
  public final OffsetMetaKey key;
  public final OffsetMsgValue value;
  public final long tip;
  
  public OffsetInfo(long tip, OffsetMetaKey key, OffsetMsgValue value) {
    this.tip = tip;
    this.key = key;
    this.value = value;
  }

  @Override
  public String toString() {
    return "OffsetInfo[tip="+tip+", key="+key+", value="+value+"]";
  }
  
  
}
