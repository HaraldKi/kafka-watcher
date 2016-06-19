package de.pifpafpuf.kawa.offmeta;

/**
 * see: https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/coordinator/GroupMetadataManager.scala
 */
public abstract class MetaKey {
  public final short version;
  
  public MetaKey(short version) {
    this.version = version;
  }
  public abstract String getKey();
  
  @Override
  public final String toString() {
    String fullname = getClass().getName();
    int pos = fullname.lastIndexOf('.');
    String name = fullname.substring(pos+1);
    return name+"[version="+version+", key="+getKey()+"]";
  }
}
