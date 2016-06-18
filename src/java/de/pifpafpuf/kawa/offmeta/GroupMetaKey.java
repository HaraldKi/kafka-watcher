package de.pifpafpuf.kawa.offmeta;

public class GroupMetaKey extends MetaKey {
  public final String group;
  
  public GroupMetaKey(short version, String group) {
    super(version);
    this.group = group;
  }

  @Override
  public String getKey() {
    return group;
  }

  public GroupMsgValue decode(byte[] data, GroupMsgValue vOld) {
    GroupMsgValue vNew = GroupMsgValue.decode(data, this);
    if (vNew==null) {
      vOld.expire();
      return vOld;
    }
    return vNew;
  }
  @Override
  public MsgValue decodeValue(byte[] value) {
    return GroupMsgValue.decode(value, this);
  }
  
}
