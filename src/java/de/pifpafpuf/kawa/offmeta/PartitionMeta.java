package de.pifpafpuf.kawa.offmeta;

public class PartitionMeta {
  public final String topic;
  public final int partition;
  public final long headOffset;
  public final long firstOffset;
  
  public PartitionMeta(String topic, int partition, 
                       long firstOffset, long headOffset) 
  {
    this.topic = topic;
    this.partition = partition;
    this.headOffset = headOffset;
    this.firstOffset = firstOffset;
  }
  
}
