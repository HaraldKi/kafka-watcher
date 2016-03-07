package de.pifpafpuf.kavi;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

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

  public void rewindOffsets(int count) {
    kafcon.seekToEnd(); // undocumented, but seeks all subscribed
    for (TopicPartition tp : kafcon.assignment()) {
      long position = kafcon.position(tp);
      if (position>0) {
        long newpos = Math.max(0, position-count);
        System.out.println("rewinding "+tp+" to "+newpos);
        kafcon.seek(tp,  newpos);
      }
    }
  }

  public Map<String, OffsetInfo> getLastOffsets(long pollMillis) {
    Map<String, OffsetInfo> result = new HashMap<>();
    ConsumerRecords<byte[], byte[]> data;
    for (data=kafcon.poll(pollMillis); 
        !data.isEmpty(); 
        data=kafcon.poll(pollMillis)) {
      for(ConsumerRecord<byte[], byte[]> r : data) {
        MetaKey key = MetaKey.decode(r.key());
        if (key instanceof OffsetMetaKey) {
          OffsetMetaKey okey = (OffsetMetaKey)key;
          OffsetMsgValue value = (OffsetMsgValue)key.decodeValue(r.value());
          result.put(key.getKey(), new OffsetInfo(okey, value));
        }
      }
    }
    return result;
  }
  
  public void listOffsets() {
    ConsumerRecords<byte[], byte[]> data;
    for (data=kafcon.poll(1000); !data.isEmpty(); data=kafcon.poll(1000)) {
      for(ConsumerRecord<byte[], byte[]> r : data) {
        MetaKey bk = MetaKey.decode(r.key());
        Object value = bk.decodeValue(r.value());
        System.out.printf("xx: partition:pos=%d:%d, key=`%s', value=`%s'%n",
                          r.partition(), r.offset(), bk, value);
      }
    }
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
  public void info() {
    Map<String,List<PartitionInfo>> meta = kafcon.listTopics();
    System.out.println("fetching information");
    for (String topic : meta.keySet()) {
      for (PartitionInfo pi : meta.get(topic)) {
        TopicPartition tp = new TopicPartition(topic, pi.partition());
        tipcon.seekToEnd(tp);
        long endPos = tipcon.position(tp);
        OffsetAndMetadata oam = kafcon.committed(tp);
        if (oam==null) {
          oam = new OffsetAndMetadata(-3);
        }
        format(pi, oam, endPos);
      }
    }
  }

  private void format(PartitionInfo pi, OffsetAndMetadata oam, long endPos) {
    if (pi.topic().startsWith("ccc__")) {
      return;
    }
    System.out.printf("topic=%s, partition=%d, leader=%s, "
        + "offset=%d, end=%d, meta=%s%n",
        pi.topic(), pi.partition(), pi.leader(),
        oam.offset(), endPos, oam.metadata());
  }

  public void run() throws InterruptedException {
    while (true) {
      info();
      Thread.sleep(2000);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    QueueWatcher qw = new QueueWatcher("localhost", 9092);
    qw.rewindOffsets(2);
    while (true) {
      Map<String, OffsetInfo> offsets = qw.getLastOffsets(1000);
      if (offsets.isEmpty()) {
        continue;
      }
      System.out.println(offsets);
    }
    
  }

}
