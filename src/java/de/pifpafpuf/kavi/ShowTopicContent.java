package de.pifpafpuf.kavi;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Comparator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import de.pifpafpuf.kavi.offmeta.GroupMetaKey;
import de.pifpafpuf.kavi.offmeta.GroupMsgValue;
import de.pifpafpuf.kavi.offmeta.MsgValue;
import de.pifpafpuf.kavi.offmeta.OffsetMetaKey;
import de.pifpafpuf.kavi.offmeta.OffsetMsgValue;
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

  /*+******************************************************************/
  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) {

    HtmlPage page = initPage("show topic content");

    String topicName = pTopic.fromFirst(req, "");
    int offset = pOffset.fromFirst(req, -5);
    QueueWatcher qw = KafkaViewerServer.getQueueWatcher();

    List<ConsumerRecord<Object, byte[]>> recs =
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
  private Html renderTable(List<ConsumerRecord<Object, byte[]>> recs) {
    Html table = new Html("table").setAttr("class", "records withdata");
    Html thead = table.add("thead");

    recs.sort(RecSorter.INSTANCE);
    Html tr =  new Html("tr").setAttr("class", "recordrow recordrowhead");
    tr.add("th").addText("created (UTC)");
    tr.add("th").addText("parti\u00adtion");
    tr.add("th").addText("offset");
    tr.add("th").addText("key");
    tr.add("th").addText("value");
    thead.add(tr);

    Html tbody = table.add("tbody");

    for (ConsumerRecord<Object, byte[]> rec : recs) {
      String created = "unknown"; //dateFormat(rec.timestamp());
      tr = new Html("tr").setAttr("class", "recordrow");
      tr.add("td").addText(created);
      tr.add("td").addText(Long.toString(rec.partition()));
      tr.add("td").addText(Long.toString(rec.offset()));
      tr.add("td").addText(rec.key().toString());
      tr.add("td").addText(convert(rec));
      tbody.add(tr);
    }
    return table;
  }
  /*+******************************************************************/
  private String convert(ConsumerRecord<Object, byte[]> rec) {
    Object key = rec.key();
    if (key instanceof GroupMetaKey) {
      MsgValue mv =GroupMsgValue.decode(rec.value());
      return mv==null ? "(null)" : mv.toString();
    }
    if (key instanceof OffsetMetaKey) {
      MsgValue mv = OffsetMsgValue.decode(rec.value());
      return mv==null ? "(null)" : mv.toString();
    }
    if (rec.value()==null) {
      return "(null message)";
    }
    try(ByteArrayInputStream bin = new ByteArrayInputStream(rec.value());
        ObjectInputStream oin = new ObjectInputStream(bin);
        ) {
      return oin.readObject().toString();
    } catch( IOException | ClassNotFoundException e ) {
      return e.getMessage();
    } 
  }
  /*+******************************************************************/
  private enum RecSorter implements Comparator<ConsumerRecord<?, ?>> {
    INSTANCE;

    @Override
    public int compare(ConsumerRecord<?,?> o1, ConsumerRecord<?,?> o2) {
      //TODO: with 0.10 we will be able to sort by timestamp 
//      if (o1.timestamp() < o2.timestamp()) {
//        return -1;
//      }
//      if (o1.timestamp() > o2.timestamp()) {
//        return 1;
//      }
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
