package de.pifpafpuf.kawa;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import de.pifpafpuf.kawa.offmeta.GroupMetaKey;
import de.pifpafpuf.kawa.offmeta.GroupMsgValue;
import de.pifpafpuf.kawa.offmeta.MetaKey;
import de.pifpafpuf.kawa.offmeta.OffsetMetaKey;
import de.pifpafpuf.kawa.offmeta.OffsetMsgValue;
import de.pifpafpuf.kawa.offmeta.OffsetsKeyDeserializer;

/**
 * continuously polls the <code>__consumer_offsets</code> and keeps the
 * current group state.
 */
public class GroupStateWatcher implements Runnable {
  private static final Logger log = KafkaWatcherServer.getLogger();
  public static final String TOPIC_OFFSET = "__consumer_offsets";
  private final KafkaConsumer<MetaKey, byte[]> offcon;
  private final KafkaConsumer<String, String> headcon;

  private final ConcurrentHashMap<String, OffsetMsgValue> stateOffsets =
      new ConcurrentHashMap<>();
  private final Map<String, OffsetMsgValue> uStateOffsets =
      Collections.unmodifiableMap(stateOffsets);

  private final ConcurrentHashMap<String, GroupMsgValue> stateGroups =
      new ConcurrentHashMap<>();
  private final Map<String, GroupMsgValue> uStateGroups =
      Collections.unmodifiableMap(stateGroups);

  private final Map<TopicPartition, Long> partitionHeads = new HashMap<>();
  private volatile boolean initializing = true;
  private AtomicLong recordsRead = new AtomicLong(0);

  /*+******************************************************************/
  public GroupStateWatcher(String hostport) {
    Properties props = new Properties();
    props.put("enable.auto.commit", "false");
    props.put("group.id", "random-"+new Random().nextInt());
    props.put("bootstrap.servers", hostport);
    offcon = new KafkaConsumer<>(props, OffsetsKeyDeserializer.INSTANCE,
                                 new ByteArrayDeserializer());

    props.put("group.id", "random-"+new Random().nextInt());
    headcon = new KafkaConsumer<>(props, new StringDeserializer(),
                                 new StringDeserializer());
  }
  /*+******************************************************************/
  public Map<String, OffsetMsgValue> getOffsetsState() {
    return uStateOffsets;
  }
  /*+******************************************************************/
  public Map<String, GroupMsgValue> getGroupsState() {
    return uStateGroups;
  }
  /*+******************************************************************/
  public boolean stillInitializing() {
    return initializing;
  }
  /*+******************************************************************/
  public long recordsRead() {
    return recordsRead.get();
  }
  /*+******************************************************************/
  public void shutdown() {
    offcon.wakeup();
  }
  /*+******************************************************************/
  @Override
  public void run() {
    while (true) {
      try {
        innerRun();
        return;
      } catch (KafkaException e) {
        log.info("bombed out, will restart", e);
      }
    }
  }
  /*+******************************************************************/
  enum State {STOP, CONTINUE}
  public void innerRun() {
    long start = System.currentTimeMillis();
    List<TopicPartition> tps = assignTopic();
    rewindOffsets(tps);
    tps = null;
    final int initialTimeout = 2000;
    if (State.STOP==processToTimeout(initialTimeout, Level.INFO)) {;
      return;
    }
    long now = System.currentTimeMillis();
    long ds = (now-start-initialTimeout);
    log.info("read "+recordsRead.get()+" records in "+ds
             +"ms before first emptying the queue");
    initializing = false;
    processToTimeout(Integer.MAX_VALUE, Level.DEBUG);
  }
  /*+******************************************************************/
  private final State processToTimeout(int timeout, Level l) {
    while (true) {
      ConsumerRecords<MetaKey, byte[]> recs = getRecords(timeout);
      if (recs==null) {
        offcon.close();
        return State.STOP;
      }
      if (recs.isEmpty()) {
        return State.CONTINUE;
      }
      if (log.isEnabledFor(l)) {
        log.log(l, "got "+recs.count()+" records");
      }
      recordsRead.addAndGet(recs.count());
      for (ConsumerRecord<MetaKey, byte[]> rec : recs.records(TOPIC_OFFSET)) {
        process(rec);
      }
      updateHeads();
    }
  }
  /*+******************************************************************/
  private final ConsumerRecords<MetaKey,byte[]> getRecords(long timeout) {
    try {
      return offcon.poll(timeout);
    } catch (WakeupException e) {
      log.info("got WakeupException, terminating");
      return null;
    } catch (KafkaException e) {
      log.error("Exiting due to total screwup, see cause", e);
      return null;
    }
  }
  /*+******************************************************************/
  private final void updateHeads() {
    Set<TopicPartition> tps = new HashSet<>();
    for (OffsetMsgValue ov : stateOffsets.values()) {
      TopicPartition tp = new TopicPartition(ov.key.topic, ov.key.partition);
      tps.add(tp);
    }
    headcon.assign(new LinkedList<>(tps));
    headcon.seekToEnd(tps.toArray(new TopicPartition[tps.size()]));
    for (TopicPartition tp : tps) {
      long head = headcon.position(tp);
      partitionHeads.put(tp, head);
    }
    for (OffsetMsgValue ov : stateOffsets.values()) {
      TopicPartition tp = new TopicPartition(ov.key.topic, ov.key.partition);
      ov.setHead(partitionHeads.get(tp));
    }
  }
  /*+******************************************************************/
  private final void process(ConsumerRecord<MetaKey, byte[]> rec) {
    MetaKey key = rec.key();
    if (key instanceof OffsetMetaKey) {
      OffsetMetaKey omkey = OffsetMetaKey.class.cast(key);
      if (!TOPIC_OFFSET.equals(omkey.topic)) {
        processOffset(omkey, rec.value());
      }
    } else if (key instanceof GroupMetaKey) {
      processGroup(GroupMetaKey.class.cast(key), rec.value());
    } else {
      log.error("got surprisingly unknown type from Kafka: "+key.getClass());
    }
  }
  /*+******************************************************************/
  private final void processOffset(OffsetMetaKey metaKey, byte[] data) {
    String key = metaKey.getKey();
    OffsetMsgValue vOld = stateOffsets.get(key);
    OffsetMsgValue vNew = metaKey.decode(data, vOld);
    if (vNew!=null) {
      stateOffsets.put(key, vNew);
    }
  }
  /*+******************************************************************/
  private final void processGroup(GroupMetaKey metaKey, byte[] data) {
    String key = metaKey.getKey();
    GroupMsgValue vOld = stateGroups.get(key);
    GroupMsgValue vNew = metaKey.decode(data, vOld);
    if (vNew!=null) {
      stateGroups.put(key, vNew);
    }
  }
  /*+******************************************************************/
  private final List<TopicPartition> assignTopic() {
    List<PartitionInfo> parts = offcon.partitionsFor(TOPIC_OFFSET);
    List<TopicPartition> tps = new LinkedList<>();
    for (PartitionInfo pi : parts) {
      tps.add(new TopicPartition(TOPIC_OFFSET, pi.partition()));
    }
    offcon.assign(tps);
    return tps;
  }
  /*+******************************************************************/
  private final void rewindOffsets(List<TopicPartition> tps) {
    for (TopicPartition tp : tps) {
      offcon.seek(tp, 0);
    }
  }

}
