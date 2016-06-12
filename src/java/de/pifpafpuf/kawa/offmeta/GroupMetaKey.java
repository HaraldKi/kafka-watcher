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

  @Override
  public MsgValue decodeValue(byte[] value) {
    return GroupMsgValue.decode(value);
  }
  
}
