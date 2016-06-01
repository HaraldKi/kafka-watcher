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
    tr.add("th").addEncoded("# par-<br/>titions");
    tr.add("th").addText("head offsets");

    List<String> sortedTopics = sortTopics(topics.keySet());
    for (String topicName : sortedTopics) {
      Html topicRow = renderTopicRow(topicName, topics.get(topicName));
      tbody.add(topicRow);
    }
    return table;
  }

  private Html renderTopicRow(String topicName, List<PartitionMeta> pmeta) {
    Html row = new Html("tr");

    StringBuilder sb = new StringBuilder(200);
    sb.append(ShowTopicContent.URL).append('?');
    ShowTopicContent.pTopic.appendToUrl(sb, topicName);
    ShowTopicContent.pOffset.appendToUrl(sb, -5);

    row.add("td")
    .add("a")
    .setAttr("href",  sb.toString())
    .addText(topicName);

    row.add("td")
    .setAttr("class", "ral")
    .addText(Integer.toString(pmeta.size()));
    Html offsets = new Html("td");

    pmeta.sort((x,y) -> x.partition-y.partition);
    for (PartitionMeta pm : pmeta) {
      offsets.addText(Long.toString(pm.headOffset));
    }
    row.add(offsets);

    return row;
  }

  private List<String> sortTopics(Set<String> topicNames) {
    List<String> result = new ArrayList<>();
    result.addAll(topicNames);
    Collections.sort(result);
    return result;
  }
}
