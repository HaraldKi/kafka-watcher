package de.pifpafpuf.kawa;

import java.io.Closeable;
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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.Logger;

import de.pifpafpuf.kawa.offmeta.PartitionMeta;
import de.pifpafpuf.util.CreateFailedException;

public class QueueWatcher implements Closeable {
  private static final Logger log = KafkaWatcherServer.getLogger();
  public static final String TOPIC_OFFSET = "__consumer_offsets";
  
  private final KafkaConsumer<Object, byte[]> kafcon;

  public QueueWatcher(String hostport) throws CreateFailedException {
    log.info("starting QueueWatcher to watch Kafka at "+hostport);
    Properties props = new Properties();
    props.put("group.id", "some-random-group-id");
    props.put("bootstrap.servers", hostport);
    props.put("enable.auto.commit", "false");
    props.put("session.timeout.ms", 5000);
    props.put("request.timeout.ms", 5001);
    long start = System.nanoTime();
    kafcon = new KafkaConsumer<>(props, GeneralKeyDeserializer.KEY,
        new ByteArrayDeserializer());
    try {
      assignAllPartitions();
    } catch (CheckedKafkaException e) {
      throw new CreateFailedException("see cause", e);
    }
    long later = System.nanoTime();
    double delta = (double)(later-start)/1000000;
    log.info("consumer initialized in "+String.format("%.3fms", delta));
  }
  /*+******************************************************************/
  @Override
  public void close() {
    log.info("closing QueueWatcher");
    kafcon.close();
  }
  /*+**********************************************************************/
  private void assignAllPartitions() throws CheckedKafkaException {
    List<TopicPartition> assigns = new LinkedList<>();
    try {
      for (String topic : kafcon.listTopics().keySet()) {
        assigns.addAll(assignablePartitions(kafcon, topic));
      }
      kafcon.assign(assigns);
    } catch (KafkaException e) {
      throw new CheckedKafkaException("could not assign partitions", e);
    }
  }
  /*+*********************************************************************/
  public Map<String, List<PartitionMeta>> topicInfo() 
      throws CheckedKafkaException 
  {
    try {
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
    } catch (KafkaException e) {
      throw new CheckedKafkaException("could not get topic infos", e);
    }
  }
  /*+******************************************************************/
  public List<ConsumerRecord<Object, byte[]>>
  readRecords(String topic, long offset, int maxRecs, Pattern pattern)
      throws CheckedKafkaException
  {
    try {
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
    } catch (KafkaException e) {
      throw new CheckedKafkaException("could not read records", e);
    }
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
