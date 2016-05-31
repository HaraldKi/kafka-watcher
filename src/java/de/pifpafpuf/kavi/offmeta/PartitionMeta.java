package de.pifpafpuf.kavi.offmeta;

public class PartitionMeta {
  public final String topic;
  public final int partition;
  public final long headOffset;
  
  public PartitionMeta(String topic, int partition, long headOffset) {
    this.topic = topic;
    this.partition = partition;
    this.headOffset = headOffset;
  }
  
}
