package de.pifpafpuf.kawa.offmeta;

import java.nio.ByteBuffer;

public class GroupMember {

  public final String memberId;
  public final String clientId;
  public final String clientHost;
  public final int sessionTimeout;
  public final ByteBuffer subscription;
  public final ByteBuffer assignment;

  public GroupMember(String memberId, String clientId, String clientHost,
                     int sessionTimeout, ByteBuffer subscription,
                     ByteBuffer assignment) {
    this.memberId = memberId;
    this.clientId = clientId;
    this.clientHost = clientHost;
    this.sessionTimeout = sessionTimeout;
    this.subscription = subscription;
    this.assignment = assignment;
  }

  @Override
  public String toString() {
    return "GroupMember [memberId="+memberId+", clientId="+clientId
        +", clientHost="+clientHost+", sessionTimeout="+sessionTimeout
        +", subscription="+subscription+", assignment="+assignment+"]";
  }

}
