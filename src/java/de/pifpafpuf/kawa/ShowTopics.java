package de.pifpafpuf.kawa;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.pifpafpuf.kawa.offmeta.PartitionMeta;
import de.pifpafpuf.web.html.Html;
import de.pifpafpuf.web.html.HtmlPage;

public class ShowTopics  extends AllServletsParent {
  public static final String URL = "/topics";

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) {
    HtmlPage page = initPage("show topics &mdash; Kavi");
    QueueWatcher qw = KafkaWatcherServer.getQueueWatcher();
    Map<String, List<PartitionMeta>> topics = qw.topicInfo();

    Locale loc = getLocale(req);
    
    page.addContent(renderHeader());
    page.addContent(renderTopics(topics, loc));
    sendPage(resp, page);
  }
  /*+******************************************************************/
  private Html renderHeader() {
    Html div = new Html("div");
    div.add("h1").addText("Kafka Topics Overview");
    return div;
  }
  /*+******************************************************************/
  private Html renderTopics(Map<String, List<PartitionMeta>> topics,
                            Locale loc) {
    Html table = new Html("table").setAttr("class",  "topiclist withdata");
    Html thead = table.add("thead");
    Html tbody = table.add("tbody");
    
    Html tr = thead.add("tr");
    tr.add("th").addText("topic name");
    tr.add("th").addEncoded("parti-<br/>tion");
//    tr.add("th").addText("first offset")
//    .setAttr("title", "unreliable data");
    tr.add("th").addEncoded("head<br/>offset");

    List<String> sortedTopics = sortTopics(topics.keySet());
    for (String topicName : sortedTopics) {
      renderTopic(tbody, topicName, topics.get(topicName), loc);
    }
    return table;
  }
  /*+******************************************************************/
  private static final 
  Comparator<PartitionMeta> byPartition = new Comparator<PartitionMeta>() {
    @Override
    public int compare(PartitionMeta pm1, PartitionMeta pm2) {
      if (pm1.partition<pm2.partition) return -1;
      if (pm1.partition>pm2.partition) return 1;
      return 0;
    }    
  };
  
  private void renderTopic(Html tbody, String topic, 
                           List<PartitionMeta> pmeta,
                           Locale loc) {
    Collections.sort(pmeta, byPartition);
    int partition = 0;
    for (PartitionMeta pm : pmeta) {
      Html row = new Html("tr");
      if (partition==0) {
        row.setAttr("class", "wtopmargin");
        StringBuilder sb = new StringBuilder(200);
        sb.append(ShowTopicContent.URL).append('?');
        ShowTopicContent.pTopic.appendToUrl(sb, topic);
        ShowTopicContent.pOffset.appendToUrl(sb, -5l);
        ShowTopicContent.pCasefold.appendToUrl(sb, true);

        row.add("td")
        .add("a")
        .setAttr("href",  sb.toString())
        .addText(topic);
      } else {
        row.add("td");
      }
      row.add("td")
      .setAttr("class", "ral")
      .addText(Integer.toString(partition++));

      Html offset = new Html("td").setAttr("class", "ral");
      offset.addText(localeFormatLong(loc, pm.headOffset));
      row.add(offset);
      tbody.add(row);
    }
  }
  /*+******************************************************************/
  private static final Comparator<String> 
  CONSUMER_OFFSET_LAST = new Comparator<String>() {
    @Override
    public int compare(String s1, String s2) {
      if (QueueWatcher.TOPIC_OFFSET.equals(s1)) return 1;
      if (QueueWatcher.TOPIC_OFFSET.equals(s2)) return -1;
      return s1.compareTo(s2);
    }
  };
  private List<String> sortTopics(Set<String> topicNames) {
    List<String> result = new ArrayList<>();
    result.addAll(topicNames);
    Collections.sort(result, CONSUMER_OFFSET_LAST);
    return result;
  }
}
