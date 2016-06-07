package de.pifpafpuf.kavi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.pifpafpuf.kavi.offmeta.PartitionMeta;
import de.pifpafpuf.web.html.Html;
import de.pifpafpuf.web.html.HtmlPage;

public class ShowTopics  extends AllServletsParent {
  public static final String URL = "/topics";

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) {
    HtmlPage page = initPage("show topics &mdash; Kavi");
    QueueWatcher qw = KafkaViewerServer.getQueueWatcher();
    Map<String, List<PartitionMeta>> topics = qw.topicInfo();

    page.addContent(renderHeader());
    page.addContent(renderTopics(topics));
    sendPage(resp, page);
  }
  /*+******************************************************************/
  private Html renderHeader() {
    Html div = new Html("div");
    div.add("h1").addText("Kafka Topics Overview");
    return div;
  }
  /*+******************************************************************/
  private Html renderTopics(Map<String, List<PartitionMeta>> topics) {
    Html table = new Html("table").setAttr("class",  "topiclist withdata");
    Html thead = table.add("thead");
    Html tbody = table.add("tbody");
    
    Html tr = thead.add("tr");
    tr.add("th").addText("topic name");
    tr.add("th").addEncoded("parti-<br/>tion");
//    tr.add("th").addText("first offset")
//    .setAttr("title", "unreliable data");
    tr.add("th").addText("head offset");

    List<String> sortedTopics = sortTopics(topics.keySet());
    for (String topicName : sortedTopics) {
      renderTopic(tbody, topicName, topics.get(topicName));
    }
    return table;
  }

  private void renderTopic(Html tbody, String topic, List<PartitionMeta> pmeta) {
    pmeta.sort((x,y) -> x.partition-y.partition);
    int partition = 0;
    for (PartitionMeta pm : pmeta) {
      Html row = new Html("tr");
      if (partition==0) {
        StringBuilder sb = new StringBuilder(200);
        sb.append(ShowTopicContent.URL).append('?');
        ShowTopicContent.pTopic.appendToUrl(sb, topic);
        ShowTopicContent.pOffset.appendToUrl(sb, -5l);

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

//      Html offset = new Html("td");
//      offset.addText(Long.toString(pm.firstOffset));
//      row.add(offset);

      Html offset = new Html("td");
      offset.addText(Long.toString(pm.headOffset));
      row.add(offset);
      tbody.add(row);
    }
  }

  private List<String> sortTopics(Set<String> topicNames) {
    List<String> result = new ArrayList<>();
    result.addAll(topicNames);
    Collections.sort(result,
                     (x,y) -> x.equals(QueueWatcher.TOPIC_OFFSET) ? 1
                         : x.compareTo(y));
    return result;
  }
}
