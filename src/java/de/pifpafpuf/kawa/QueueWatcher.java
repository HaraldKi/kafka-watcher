package de.pifpafpuf.kawa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.Logger;

import de.pifpafpuf.kawa.offmeta.GroupMetaKey;
import de.pifpafpuf.kawa.offmeta.GroupMsgValue;
import de.pifpafpuf.kawa.offmeta.MetaKey;
import de.pifpafpuf.kawa.offmeta.OffsetInfo;
import de.pifpafpuf.kawa.offmeta.OffsetMetaKey;
import de.pifpafpuf.kawa.offmeta.OffsetMsgValue;
import de.pifpafpuf.kawa.offmeta.OffsetsKeyDeserializer;
import de.pifpafpuf.kawa.offmeta.PartitionMeta;

public class QueueWatcher {
  private static final Logger log = KafkaWatcherServer.getLogger();
  public static final String TOPIC_OFFSET = "__consumer_offsets";

  private final KafkaConsumer<Object, byte[]> kafcon;
  private final KafkaConsumer<MetaKey, byte[]> offcon;

  // FIXME: we are using KafkaConsumer in a non thread-safe fashion. Either
  // allocate a new one each time or use a ThreadLocal. But ThreadLocal does
  // not seem to provide a means for controlled freeing/closing of resources.
  // What a nonsense.
  
  // TODO: don't always read the __consumer_offsets from some arbitrary
  // offset backwards. Rather start polling and provide a consolidated status
  // in-memory any time.
  
  public QueueWatcher(String hostport) {
    Properties props = new Properties();
    props.put("group.id", "some-random-group-id");
    props.put("bootstrap.servers", hostport);
    props.put("enable.auto.commit", "false");
    props.put("log.message.format.version", "0.9.0");
    kafcon = new KafkaConsumer<>(props, GeneralKeyDeserializer.KEY,
        new ByteArrayDeserializer());
    assignAllPartitions(kafcon);
    props.put("group.id", "totally-random-group-id");

    offcon = new KafkaConsumer<>(props, OffsetsKeyDeserializer.INSTANCE,
        new ByteArrayDeserializer());
    offcon.assign(assignablePartitions(offcon, TOPIC_OFFSET));
  }
  /*+******************************************************************/
  public void shutdown() {
    kafcon.close();
    offcon.close();
  }
  /*+**********************************************************************/
  private static void assignAllPartitions(KafkaConsumer<?,?> consumer) {
    List<TopicPartition> assigns = new LinkedList<>();
    for (String topic : consumer.listTopics().keySet()) {
      assigns.addAll(assignablePartitions(consumer, topic));
    }
    consumer.assign(assigns);
  }
  /*+*********************************************************************/
  public Map<String, List<PartitionMeta>> topicInfo() {
    Map<String, List<PartitionMeta>> result = new HashMap<>();
    Map<String, List<PartitionInfo>> m = kafcon.listTopics();

    for (Map.Entry<String, List<PartitionInfo>> elem : m.entrySet()) {
      List<PartitionMeta> l = new LinkedList<>();
      String topic = elem.getKey();
      result.put(topic, l);
      setOffsets(topic, -1);
      for (PartitionInfo pi : elem.getValue()) {
        TopicPartition tp = tpFromPi(pi);
        long headOffset = kafcon.position(tp);
        kafcon.seek(tp , 0);
        //kafcon.poll(100);
        long firstOffset = kafcon.position(tp);
        l.add(new PartitionMeta(pi.topic(), pi.partition(),
                                firstOffset, headOffset+1));
      }
    }
    return result;
  }
  /*+******************************************************************/
  public List<ConsumerRecord<Object, byte[]>>
  readRecords(String topic, long offset, int maxRecs, Pattern pattern)
  {
    setOffsets(topic, offset);

    int numPartitions = kafcon.partitionsFor(topic).size();
    maxRecs *= numPartitions;
    Matcher m = pattern.matcher("");
    List<ConsumerRecord<Object, byte[]>> result = new LinkedList<>();
    final long WAIT = 1000;
    boolean timedout = false;
    while (!timedout && result.size()<maxRecs) {
      long now = System.currentTimeMillis();
      ConsumerRecords<Object, byte[]> recs = kafcon.poll(WAIT);
      long later = System.currentTimeMillis();
      timedout = now+WAIT<=later;
      if (log.isDebugEnabled()) {
        log.debug("got "+recs.count()+" records : after "+(later-now)+"ms"
            + " for "+recs.partitions()+", timedout="+timedout);
      }
      roundRobinExtract(result, recs, maxRecs, m);
    }
    return result;
  }
  /*+******************************************************************/
  private void roundRobinExtract(List<ConsumerRecord<Object,byte[]>> result,
                                 ConsumerRecords<Object,byte[]> recs,
                                 int maxRecs,
                                 Matcher m)
  {
    List<TopicPartition> tps = new LinkedList<>();
    tps.addAll(recs.partitions());
    List<List<ConsumerRecord<Object, byte[]>>> partRecs = new ArrayList<>();
    for (TopicPartition tp : recs.partitions()) {
      List<ConsumerRecord<Object, byte[]>> l = new LinkedList<>();
      l.addAll(recs.records(tp));
      partRecs.add(l);
    }
    int currentPart = 0;
    while (result.size()<maxRecs && partRecs.size()>0)  {
      currentPart = currentPart % partRecs.size();
      List<ConsumerRecord<Object, byte[]>> l = partRecs.get(currentPart);
      ConsumerRecord<Object, byte[]> rec = l.remove(0);
      m.reset(rec.key().toString());
      if (l.isEmpty()) {
        partRecs.remove(currentPart);
      } else {
        currentPart += 1;
      }

      if (m.find()) {
        result.add(rec);
      }
    }
  }
  /*+******************************************************************/
  private void setOffsets(String topic, long offset) {
    int numPartitions = kafcon.partitionsFor(topic).size();
    List<TopicPartition> tps = new LinkedList<>();
    for (int i=0; i<numPartitions; i++) {
      tps.add(new TopicPartition(topic, i));
    }

    kafcon.assign(tps);
    if (offset<0) {
      //TODO: for 0.10.0 kafcon.seekToEnd(tps);
      kafcon.seekToEnd(tps.toArray(new TopicPartition[tps.size()]));
    }
    for (int i=0; i<numPartitions; i++) {
      TopicPartition tp = tps.remove(0);
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
  private long getHead(OffsetMetaKey okey) {
    TopicPartition tp = new TopicPartition(okey.topic, okey.partition);
    //TODO: for 0.10.0 kafcon.seekToEnd(Collections.singletonList(tp));
    kafcon.seekToEnd(tp);
    return kafcon.position(tp);
  }
  /*+**********************************************************************/
  public void rewindOffsets(int count) {
    List<TopicPartition> tps = assignablePartitions(offcon, TOPIC_OFFSET);
    //TODO: for 0.10.0 offcon.seekToEnd(tps);
    offcon.seekToEnd(tps.toArray(new TopicPartition[tps.size()]));
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
    Map<String, List<String>> groups = new HashMap<>();
    assignAllPartitions(kafcon);
    Map<String, OffsetInfo> curerentState = new HashMap<>();
    ConsumerRecords<MetaKey, byte[]> data;

    // TODO: There is more data to decode for group messages.
    
    for (data=offcon.poll(pollMillis);
        !data.isEmpty();
        data=offcon.poll(pollMillis)) {
      for(ConsumerRecord<MetaKey, byte[]> r : data) {
        updateState(groups, curerentState, r);
      }
    }
    return curerentState;
  }
  /*+******************************************************************/
  private void updateState(Map<String,List<String>> groups,
                           Map<String,OffsetInfo> currentState,
                           ConsumerRecord<MetaKey,byte[]> rec)
  {
    MetaKey key = rec.key();

    if (key instanceof OffsetMetaKey) {
      OffsetMetaKey okey = (OffsetMetaKey)key;
      OffsetMsgValue value = (OffsetMsgValue)key.decodeValue(rec.value());
      long tip = getHead(okey);
      OffsetInfo oinfo = new OffsetInfo(tip, okey, value);
      OffsetInfo latest = currentState.get(key.getKey());
      if (latest==null) {
        currentState.put(key.getKey(), oinfo);
      } else {
        latest.setValue(value);
      }
      addConsumer(groups, okey);
      return;
    }

    GroupMetaKey gkey = (GroupMetaKey)key;
    boolean dead = null==GroupMsgValue.decode(rec.value(), gkey);
    List<String> groupKeys = groups.get(gkey.group);
    if (groupKeys==null) {
      return;
    }
    for (String member : groupKeys) {
      OffsetInfo oi = currentState.get(member);
      oi.setDead(dead);
    }
  }
  /*+**********************************************************************/
  private static void addConsumer(Map<String, List<String>> groups,
                                  OffsetMetaKey okey) {
    List<String> keys = groups.get(okey.group);
    if (keys==null) {
      keys = new LinkedList<String>();
      groups.put(okey.group, keys);
    }
    keys.add(okey.getKey());
  }
  /*+**********************************************************************/
  private static List<TopicPartition>
  assignablePartitions(KafkaConsumer<?,?> con, String topic)
  {
    List<TopicPartition> result = new LinkedList<>();
    List<PartitionInfo> pis = con.partitionsFor(topic);
    for (PartitionInfo pi : pis) {
      result.add(tpFromPi(pi));
    }
    return result;
  }
  private static TopicPartition tpFromPi(PartitionInfo pi) {
    return new TopicPartition(pi.topic(), pi.partition());
  }
}
