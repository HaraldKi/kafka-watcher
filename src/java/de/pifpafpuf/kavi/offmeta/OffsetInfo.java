package de.pifpafpuf.kavi.offmeta;

public class OffsetInfo {
  public final OffsetMetaKey key;
  public final long tip;
  private OffsetMsgValue value;
  private boolean dead;
  private boolean closed;

  public OffsetInfo(long tip, OffsetMetaKey key, OffsetMsgValue value) {
    this.tip = tip;
    this.key = key;
    this.value = value;
    this.dead = false;
    this.closed = value==null;
  }

  @Override
  public String toString() {
    return "OffsetInfo[tip="+tip+", key="+key+", value="+value
        +", closed="+closed+", dead="+dead+"]";
  }

  public OffsetMsgValue getValue() {
    return value;
  }

  public void setValue(OffsetMsgValue value) {
    if (value==null) {
      this.closed = true;
    } else {
      this.value = value;
      this.closed = false;
    }
  }

  public void setDead(boolean state) {
    dead = state;
  }
  public boolean isDead() {
    return dead;
  }

  public boolean isClosed() {
    return closed;
  }

}
