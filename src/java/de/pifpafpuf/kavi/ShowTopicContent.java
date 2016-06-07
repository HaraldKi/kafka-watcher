package de.pifpafpuf.kavi;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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
import de.pifpafpuf.web.urlparam.BooleanCodec;
import de.pifpafpuf.web.urlparam.IntegerCodec;
import de.pifpafpuf.web.urlparam.LongCodec;
import de.pifpafpuf.web.urlparam.StringCodec;
import de.pifpafpuf.web.urlparam.UrlParamCodec;

public class ShowTopicContent  extends AllServletsParent {
  public static final String URL = "/topic";

  public static final UrlParamCodec<String> pTopic =
      new UrlParamCodec<>("topic", StringCodec.INSTANCE);
  public static final UrlParamCodec<Long> pOffset =
      new UrlParamCodec<>("offset", LongCodec.INSTANCE);
  public static final UrlParamCodec<Integer> pRefreshSecs =
      new UrlParamCodec<>("refreshsecs",
                          new IntegerCodec(1, Integer.MAX_VALUE));  
  public static final UrlParamCodec<String> pRegex = 
      new UrlParamCodec<>("regex", StringCodec.INSTANCE);

  public static final UrlParamCodec<Integer> pMax =
      new UrlParamCodec<>("maxrecs", 
                          new IntegerCodec(1, Integer.MAX_VALUE));
  
  public static final UrlParamCodec<Boolean> pCasefold =
      new UrlParamCodec<>("icase", BooleanCodec.INSTANCE);
  
  /*+******************************************************************/
  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) {

    HtmlPage page = initPage("show topic content");
    int refreshSecs = pRefreshSecs.fromFirst(req, -1);
    addRefreshMeta(page, refreshSecs);
    String topicName = pTopic.fromFirst(req, "");
    long offset = pOffset.fromFirst(req, -5l);
    int maxRecs = pMax.fromFirst(req, 5);
    
    boolean caseFold = pCasefold.fromFirst(req, false);
    Pattern pattern = null;
    String regex = pRegex.fromFirst(req, ".");
    String eMsg = null;
    int flags = caseFold ? Pattern.CASE_INSENSITIVE : 0;
    try {
      pattern = Pattern.compile(regex, flags);
    } catch (PatternSyntaxException e) {
      eMsg = e.getMessage();
    }
    
    QueueWatcher qw = KafkaViewerServer.getQueueWatcher();
    
    List<ConsumerRecord<Object, byte[]>> recs =
        qw.readRecords(topicName, offset, maxRecs, pattern);

    page.addContent(renderRefreshButton(refreshSecs, topicName, offset));
    page.addContent(renderHeader(topicName));
    page.addContent(renderForm(topicName, regex, caseFold, 
                               eMsg, offset, maxRecs));
    page.addContent(renderTable(recs));
    sendPage(resp, page);
  }
  /*+******************************************************************/
  private Html renderHeader(String topic) {
    return new Html("h1")
        .addText("Keys from topic:")
        .addText(topic);
  }
  /*+******************************************************************/
  private Html renderRefreshButton(int refreshSecs, String topic, long offset) {
    StringBuilder sb = new StringBuilder(200);
    sb.append(ShowTopicContent.URL).append('?');
    pTopic.appendToUrl(sb, topic);
    pOffset.appendToUrl(sb, offset);

    Html a = renderRefreshButton(refreshSecs, sb, pRefreshSecs);
    return a;
  }
  /*+******************************************************************/
  private Html renderForm(String topic, String regex, boolean caseFold, 
                          String eMsg, long offset, int maxRecs) {
    Html form = new Html("form")
        .setAttr("action", URL)
        .setAttr("method", "GET")
        .setAttr("class", "grepform");

    Html hiddenTopic = form.add("input")
        .setAttr("type", "hidden");
    pTopic.setParam(hiddenTopic, topic);
    if (eMsg!=null) {
      Html p = form.add("p")
          .addText("regular expression could not be compiled:");
      p.add("span").setAttr("class", "emsg").addText(eMsg);
    }
    
    Html regexDiv = form.add("div");
    regexDiv.add("div").addText("Regex");
    Html regexInput = regexDiv.add("input").setAttr("type", "text");
    pRegex.setParam(regexInput, regex);
    
    Html casefoldDiv = form.add("div");
    casefoldDiv.add("div").addText("case insensitive");
    Html casefoldInput = casefoldDiv.add("input")
        .setAttr("type", "checkbox");
    pCasefold.setParam(casefoldInput, true);
    if (caseFold) {
      casefoldInput.setAttr("checked", "");
    }
    
    Html offsetDiv = form.add("div");
    offsetDiv.add("div").addText("Offset");
    Html offsetInput = offsetDiv.add("input")
    .setAttr("type", "text")
    .setAttr("title", "use negative offsets to count from the head of the log");
    pOffset.setParam(offsetInput, offset);
    
    Html maxDiv = form.add("div");
    maxDiv.add("div").addText("max records to fetch");
    Html maxSelect = maxDiv.add("select")
        .setAttr("name", "maxrecs");
    for (int maxOpt : new int[] {5, 10, 20, 50, 100}) {
      Html option = maxSelect.add("option")
          .setAttr("value", Integer.toString(maxOpt))
          .addText(Integer.toString(maxOpt));
      if (maxOpt>=maxRecs) {
        option.setAttr("selected", "");
        maxRecs = Integer.MAX_VALUE;
      }
    }
    
    form.add("input")
    .setAttr("type", "submit")
    .setAttr("name", "submit")
    .setAttr("value", "grep");;
    
    return form;
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
    } catch (IOException | ClassNotFoundException e) {
      return e.getClass().getName()+": "+e.getMessage();
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
