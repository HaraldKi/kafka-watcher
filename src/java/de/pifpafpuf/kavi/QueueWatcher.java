package de.pifpafpuf.kavi;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import de.pifpafpuf.kavi.offmeta.GroupMetaKey;
import de.pifpafpuf.kavi.offmeta.GroupMsgValue;
import de.pifpafpuf.kavi.offmeta.MetaKey;
import de.pifpafpuf.kavi.offmeta.OffsetInfo;
import de.pifpafpuf.kavi.offmeta.OffsetMetaKey;
import de.pifpafpuf.kavi.offmeta.OffsetMsgValue;
import de.pifpafpuf.kavi.offmeta.OffsetsKeyDeserializer;
import de.pifpafpuf.kavi.offmeta.PartitionMeta;

public class QueueWatcher {
  private static final Logger log = KafkaViewerServer.getLogger();
  private final String TOPIC_OFFSET = "__consumer_offsets";

  private final KafkaConsumer<String, byte[]> kafcon;
  private final KafkaConsumer<MetaKey, byte[]> offcon;

  public QueueWatcher(String host, int port) {
    Properties props = new Properties();
    props.put("group.id", "some-random-group-id");
    props.put("bootstrap.servers", host+":"+port);
    props.put("enable.auto.commit", "false");
    kafcon = new KafkaConsumer<>(props, new StringDeserializer(),
        new ByteArrayDeserializer());
    assignAllPartitions(kafcon);
    props.put("group.id", "totally-random-group-id");
    
    offcon = new KafkaConsumer<>(props, OffsetsKeyDeserializer.INSTANCE,
        new ByteArrayDeserializer());
    List<TopicPartition> tps = new LinkedList<>();
    assignablePartitions(tps, offcon, TOPIC_OFFSET);
    offcon.assign(tps);
  }
  /*+**********************************************************************/
  public Map<String, List<PartitionMeta>> topicInfo() {
    Map<String, List<PartitionMeta>> result = new HashMap<>();
    Map<String, List<PartitionInfo>> m = kafcon.listTopics();
    for (Map.Entry<String, List<PartitionInfo>> elem : m.entrySet()) {
      List<PartitionMeta> l = result.get(elem.getKey());
      if (l==null) {
        l = new LinkedList<>();
        result.put(elem.getKey(), l);
        setOffsets(elem.getKey(), -1);
      }
      for (PartitionInfo pi : elem.getValue()) {
        long offset = kafcon.position(tpFromPi(pi));
        l.add(new PartitionMeta(pi.topic(), pi.partition(), offset+1));
      }
    }
    return result;
  }
  /*+******************************************************************/
  public List<ConsumerRecord<String, byte[]>>
  readRecords(String topic, int offset)
  {
    setOffsets(topic, offset);
    final long WAIT = 1000;
    boolean timedout = false;
    List<ConsumerRecord<String, byte[]>> result = new LinkedList<>();
    while (!timedout) {
      long now = System.currentTimeMillis();
      ConsumerRecords<String, byte[]> recs = kafcon.poll(WAIT);
      long later = System.currentTimeMillis();
      timedout = now+WAIT>=later;
      for (ConsumerRecord<String, byte[]> rec : recs) {
        result.add(rec);
      }
    }
    return result;
  }
  /*+******************************************************************/
  private void setOffsets(String topic, int offset) {
    int numPartitions = kafcon.partitionsFor(topic).size();
    List<TopicPartition> result = new LinkedList<>();
    for (int i=0; i<numPartitions; i++) {
      result.add(new TopicPartition(topic, i));
    }
    if (offset<0) {
      kafcon.seekToEnd(result);
    }
    
    for (int i=0; i<numPartitions; i++) {
      TopicPartition tp = result.remove(0);
      long newOffset; 
      if (offset<0) {
        newOffset = Math.max(0, kafcon.position(tp)+offset);
      } else {
        newOffset = offset;
      }
      kafcon.seek(tp, newOffset);
    }
  }
  /*+******************************************************************/
  private long getHead(TopicPartition key) {
    offcon.seekToEnd(Collections.singletonList(key));
    return offcon.position(key);
  }
  /*+**********************************************************************/
  public void rewindOffsets(int count) {
    List<TopicPartition> tps = new LinkedList<>();
    assignablePartitions(tps, offcon, TOPIC_OFFSET);
    offcon.seekToEnd(tps);
    for (TopicPartition tp : offcon.assignment()) {
      long position = offcon.position(tp);
      if (position>0) {
        long newpos = Math.max(0, position-count);
        log.info("seeking "+tp+" to "+newpos);
        offcon.seek(tp,  newpos);
      }
    }
  }
  /*+**********************************************************************/
  public Map<String, OffsetInfo> getLastOffsets(long pollMillis) {
    Map<String, List<String>> groupData = new HashMap<>();

    Map<String, OffsetInfo> result = new HashMap<>();
    ConsumerRecords<MetaKey, byte[]> data;
    for (data=offcon.poll(pollMillis);
        !data.isEmpty();
        data=offcon.poll(pollMillis)) {
      for(ConsumerRecord<MetaKey, byte[]> r : data) {
        MetaKey key = r.key();
        result.remove(key.getKey()); // keep only the most recent
        if (key instanceof OffsetMetaKey) {
          OffsetMetaKey okey = (OffsetMetaKey)key;
          OffsetMsgValue value = (OffsetMsgValue)key.decodeValue(r.value());
          long tip = 0;//getHead(okey);
          OffsetInfo oinfo = new OffsetInfo(tip, okey, value);
          result.put(key.getKey(), oinfo);
          addConsumer(groupData, okey);
        } else {
          GroupMsgValue v = GroupMsgValue.decode(r.value());
          GroupMetaKey gkey = (GroupMetaKey)key;
          if (v.version<0) {
            List<String> deadKeys = groupData.get(gkey.group);
            if (deadKeys!=null) {
              for (String dead : deadKeys) {
                OffsetInfo oi = result.remove(dead);
                result.put(dead, oi.asDead());
              }
            }
          }
        }
      }
    }
    return result;
  }
  /*+**********************************************************************/
  private static void addConsumer(Map<String, List<String>> groupData,
                                  OffsetMetaKey okey) {
    List<String> keys = groupData.get(okey.group);
    if (keys==null) {
      keys = new LinkedList<String>();
      groupData.put(okey.group, keys);
    }
    keys.add(okey.getKey());
  }
  /*+**********************************************************************/
  private static void assignablePartitions(List<TopicPartition> result,
                                           KafkaConsumer<?,?> con,
                                           String topic)
  {
    List<PartitionInfo> pis = con.partitionsFor(topic);
    for (PartitionInfo pi : pis) {
      result.add(tpFromPi(pi));
    }
  }
  private static TopicPartition tpFromPi(PartitionInfo pi) {
    return new TopicPartition(pi.topic(), pi.partition());
  }

  private static void assignAllPartitions(KafkaConsumer<?,?> consumer) {
    List<TopicPartition> assigns = new LinkedList<>();
    for (String topic : consumer.listTopics().keySet()) {
      assignablePartitions(assigns, consumer, topic);
    }
    consumer.assign(assigns);
  }
  /*+******************************************************************/
  private enum OffsetCmp implements Comparator<OffsetInfo> {
    INSTANCE;

    @Override public int compare(OffsetInfo arg0, OffsetInfo arg1) {
      OffsetMetaKey k0 = arg0.key;
      OffsetMetaKey k1 = arg1.key;

      int v = k0.topic.compareTo(k1.topic);
      if (v!=0) {
        return v;
      }
      v = k0.partition - k1.partition;
      if (v!=0) {
        return v;
      }
      return k0.group.compareTo(k1.group);
    }
  }
  /*+**********************************************************************/
  private static final void prettyPrint(Map<String, OffsetInfo> offsets) {
    DateFormat df = new SimpleDateFormat("yyyyMMdd_HH:mm:ss");

    List<OffsetInfo> l = new ArrayList<>(offsets.size());
    l.addAll(offsets.values());
    Collections.sort(l, OffsetCmp.INSTANCE);
    for (OffsetInfo oi : l) {
      OffsetMetaKey key = oi.key;
      OffsetMsgValue value = oi.value;
      String commit = df.format(value.commitStamp);
      long lag = oi.tip-value.offset;
      String msg = String.format("%s.%02d/%s: offset=%5d of %s, lag=%d%n",
                                 key.topic, key.partition, key.group,
                                 value.offset, commit, lag);
      System.out.print(msg);
    }
  }
  /*+**********************************************************************/
}
