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

import de.pifpafpuf.kavi.offmeta.GroupMetaKey;
import de.pifpafpuf.kavi.offmeta.GroupMsgValue;
import de.pifpafpuf.kavi.offmeta.MetaKey;
import de.pifpafpuf.kavi.offmeta.MsgValue;
import de.pifpafpuf.kavi.offmeta.OffsetInfo;
import de.pifpafpuf.kavi.offmeta.OffsetMetaKey;
import de.pifpafpuf.kavi.offmeta.OffsetMsgValue;

public class QueueWatcher  {
  private final String TOPIC_OFFSET = "__consumer_offsets";

  private final KafkaConsumer<byte[], byte[]> kafcon;
  private final KafkaConsumer<String, String> tipcon;

  public QueueWatcher(String host, int port) {
    Properties props = new Properties();
    props.put("group.id", "some-random-group-id");
    props.put("bootstrap.servers", host+":"+port);
    props.put("enable.auto.commit", "false");
    kafcon = new KafkaConsumer<>(props, new ByteArrayDeserializer(),
        new ByteArrayDeserializer());
    List<TopicPartition> tps = new LinkedList<>();
    assignablePartitions(tps, kafcon, TOPIC_OFFSET);
    kafcon.assign(tps);

    props.put("group.id", "this-should-be-a-random-group");
    tipcon = new KafkaConsumer<>(props, new StringDeserializer(),
        new StringDeserializer());
    //tipcon.subscribe(Pattern.compile(".*"), this);
    assignAllTipCon();
  }
  /*+**********************************************************************/
  public void rewindOffsets(int count) {
    kafcon.seekToEnd(); // undocumented, but seeks all subscribed
    for (TopicPartition tp : kafcon.assignment()) {
      long position = kafcon.position(tp);
      if (position>0) {
        long newpos = Math.max(0, position-count);
        kafcon.seek(tp,  newpos);
      }
    }
  }
  /*+**********************************************************************/
  public Map<String, OffsetInfo> getLastOffsets(long pollMillis) {
    Map<String, List<String>> groupData = new HashMap<>();
    
    Map<String, OffsetInfo> result = new HashMap<>();
    ConsumerRecords<byte[], byte[]> data;
    for (data=kafcon.poll(pollMillis);
        !data.isEmpty();
        data=kafcon.poll(pollMillis)) {
      for(ConsumerRecord<byte[], byte[]> r : data) {
        MetaKey key = MetaKey.decode(r.key());
        result.remove(key.getKey());
        if (key instanceof OffsetMetaKey) {
          OffsetMetaKey okey = (OffsetMetaKey)key;
          OffsetMsgValue value = (OffsetMsgValue)key.decodeValue(r.value());
          long tip = getLogTip(okey);
          OffsetInfo oinfo = new OffsetInfo(tip, okey, value);
          result.put(key.getKey(), oinfo);
          addConsumer(groupData, okey);
          //System.out.printf("%s  %s%n", key, value);
        } else {
          GroupMsgValue v = GroupMsgValue.decode(r.value());
          if (v.version<0) {
            GroupMetaKey gkey = (GroupMetaKey)key;
            //System.out.println("killing "+gkey);
            List<String> deadKeys = groupData.get(gkey.group);
            if (deadKeys!=null) {
              for (String dead : deadKeys) {
                result.remove(dead);
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
  private long getLogTip(OffsetMetaKey key) {
    TopicPartition tp = new TopicPartition(key.topic, key.partition);
    tipcon.seekToEnd(tp);
    return tipcon.position(tp);
  }

  private void assignablePartitions(List<TopicPartition> result,
                                    KafkaConsumer<?,?> con,
                                    String topic)
  {
    List<PartitionInfo> pis = con.partitionsFor(topic);
    for (PartitionInfo pi : pis) {
      result.add(tpFromPi(pi));
    }
  }
  private TopicPartition tpFromPi(PartitionInfo pi) {
    return new TopicPartition(pi.topic(), pi.partition());
  }

  private void assignAllTipCon() {
    List<TopicPartition> assigns = new LinkedList<>();
    for (String topic : tipcon.listTopics().keySet()) {
      assignablePartitions(assigns, tipcon, topic);
    }
    tipcon.assign(assigns);
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
  public static void main(String[] args)  {
    QueueWatcher qw = new QueueWatcher("localhost", 9092);

    while (true) {
      qw.rewindOffsets(100);
      Map<String, OffsetInfo> offsets = qw.getLastOffsets(1000);
      if (offsets.isEmpty()) {
        continue;
      }
      System.out.println();
      prettyPrint(offsets);
    }

  }

}
