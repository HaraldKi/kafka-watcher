package de.pifpafpuf.kavi.offmeta;

public class OffsetInfo {
  public final OffsetMetaKey key;
  public final OffsetMsgValue value;
  public final long tip;
  public final boolean dead;
  
  public OffsetInfo(long tip, OffsetMetaKey key, OffsetMsgValue value) {
    this(tip, key, value, false);
  }

  private OffsetInfo(long tip, OffsetMetaKey key, 
                     OffsetMsgValue value, boolean dead) {
    this.tip = tip;
    this.key = key;
    this.value = value;
    this.dead = dead;
  }
  
  public OffsetInfo asDead() {
    return new OffsetInfo(tip, key, value, true);
  }
  @Override
  public String toString() {
    return "OffsetInfo[tip="+tip+", key="+key+", value="+value
        +", dead="+dead+"]";
  }
  
  
}
