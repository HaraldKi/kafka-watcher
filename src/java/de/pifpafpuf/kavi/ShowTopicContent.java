package de.pifpafpuf.kavi;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import de.pifpafpuf.web.html.Html;
import de.pifpafpuf.web.html.HtmlPage;
import de.pifpafpuf.web.urlparam.IntegerCodec;
import de.pifpafpuf.web.urlparam.StringCodec;
import de.pifpafpuf.web.urlparam.UrlParamCodec;

public class ShowTopicContent  extends AllServletsParent {
  public static final String URL = "/topic";
  
  public static final UrlParamCodec<String> pTopic = 
      new UrlParamCodec<>("topic", StringCodec.INSTANCE);
  public static final UrlParamCodec<Integer> pOffset = 
      new UrlParamCodec<>("offset", IntegerCodec.INSTANCE);
  
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final DateTimeFormatter dtf =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  /*+******************************************************************/
  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) {
    
    HtmlPage page = initPage("show topic content");
    
    String topicName = pTopic.fromFirst(req, "");
    int offset = pOffset.fromFirst(req, -5);
    QueueWatcher qw = KafkaViewerServer.getQueueWatcher();
    
    List<ConsumerRecord<String, byte[]>> recs = 
        qw.readRecords(topicName, offset);
        
    page.addContent(renderHeader(topicName));    
    page.addContent(renderTable(recs));
    sendPage(resp, page);
  }
  /*+******************************************************************/
  private Html renderHeader(String topic) {
    return new Html("h1")
        .addText("Latest keys from topic:")
        .addText(topic);
  }
  /*+******************************************************************/
  private Html renderTable(List<ConsumerRecord<String, byte[]>> recs) {
    Html table = new Html("table").setAttr("class", "records");
    Html thead = table.add("thead");
    
    recs.sort(RecSorter.INSTANCE);
    Html tr =  new Html("tr").setAttr("class", "recordrow recordrowhead");
    tr.add("td").addText("created (UTC)");
    tr.add("td").addText("parti\u00adtion");
    tr.add("td").addText("offset");
    tr.add("td").addText("key");
    thead.add(tr);
    
    Html tbody = table.add("tbody");
    
    for (ConsumerRecord<String, byte[]> rec : recs) {
      String created = getCreated(rec.timestamp());
      tr = new Html("tr").setAttr("class", "recordrow");
      tr.add("td").addText(created);
      tr.add("td").addText(Long.toString(rec.partition()));
      tr.add("td").addText(Long.toString(rec.offset()));
      tr.add("td").addText(rec.key());
      tbody.add(tr);
    }
    return table;
  }
  private String getCreated(long timestamp) {
    Instant stamp = Instant.ofEpochMilli(timestamp);
    ZonedDateTime d = ZonedDateTime.ofInstant(stamp, UTC);
    return dtf.format(d);
  }
  /*+******************************************************************/
  private enum RecSorter implements Comparator<ConsumerRecord<?, ?>> {
    INSTANCE;

    @Override
    public int compare(ConsumerRecord<?,?> o1, ConsumerRecord<?,?> o2) {
      if (o1.timestamp() < o2.timestamp()) {
        return -1;
      }
      if (o1.timestamp() > o2.timestamp()) {
        return 1;
      }

      
      if (o1.partition() < o2.partition()) {
        return -1;
      }
      if (o1.partition() > o2.partition()) {
        return 1;
      }
      if (o1.offset() < o2.offset()) {
        return -1;
      }
      if (o1.offset() > o2.offset()) {
        return 1;
      }
      return 0;
    }
    
  }
}
