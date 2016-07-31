package de.pifpafpuf.kawa.offmeta;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.protocol.types.Type;

public class GroupMsgValue {
  // only partially implemented
  public final short version;
  public final String protocolType;
  public final GroupMetaKey key;
  private long expired = -1;
  public final String leaderId;
  public final int generationId;
  public final String protocol;
  public final List<GroupMember> members;

  public GroupMsgValue(short version, String protocolType,
                       int generationId, String leaderId, String protocol,
                       List<GroupMember> members,
                       GroupMetaKey key) {
    this.version = version;
    this.protocolType = protocolType;
    this.generationId = generationId;
    this.leaderId = leaderId;
    this.protocol = protocol;
    this.members = Collections.unmodifiableList(members);
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
      // order according to kafka/coordinator/GroupMetadataManager.scala:722
      // GROUP_METADATA_VALUE_SCHEMA_V0
      String protocolType = (String)Type.STRING.read(b);
      int generationId = (Integer)Type.INT32.read(b);
      String protocol = (String)Type.NULLABLE_STRING.read(b);
      if (protocol==null) {
        protocol = "";
      }
      String leaderId = (String)Type.NULLABLE_STRING.read(b);
      if (leaderId==null) {
        leaderId = "";
      }
      List<GroupMember> members = readMemberArray(b);

      return new GroupMsgValue(version, protocolType,
                               generationId, leaderId, protocol,
                               members,
                               key);
    }

    return new GroupMsgValue(version, "wrong version",
                             -1, null, null,
                             Collections.<GroupMember>emptyList(),
                             key);
  }
  /*+******************************************************************/
  private static List<GroupMember> readMemberArray(ByteBuffer b) {
    int size = (Integer)Type.INT32.read(b);
    List<GroupMember> result = new ArrayList<>(size);
    for (int i=0; i<size; i++) {
      GroupMember gm = readGroupMember(b);
      result.add(gm);
    }
    return result;
  }
  /*+******************************************************************/
  private static GroupMember readGroupMember(ByteBuffer b) {
    String memberId = (String)Type.STRING.read(b);
    String clientId = (String)Type.STRING.read(b);
    String clientHost = (String)Type.STRING.read(b);
    int sessionTimeout = (Integer)Type.INT32.read(b);
    ByteBuffer subscription = (ByteBuffer)Type.BYTES.read(b);
    ByteBuffer assignment = (ByteBuffer)Type.BYTES.read(b);
    return new GroupMember(memberId, clientId, clientHost,
                          sessionTimeout, subscription, assignment);
  }
  /*+******************************************************************/
  @Override
  public String toString() {
    return "GroupMsgValue[version="+version+", protocol="+protocolType+"]";
  }


}
