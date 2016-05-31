package de.pifpafpuf.kavi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kafka.common.PartitionInfo;

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
    Html div = renderTopics(topics);
    
    page.addContent(div);
    sendPage(resp, page);
  }
  
  private Html renderTopics(Map<String, List<PartitionMeta>> topics) {
    Html div = new Html("div")
        .setAttr("class",  "topiclist");
    
    Html thead = new Html("div").setAttr("class", "topicrow topicrowhead");
    thead.add("div").addText("topic name");
    thead.add("div").addEncoded("# par-<br/>titions");
    thead.add("div").addText("head offsets");
    div.add(thead);
    
    
    List<String> sortedTopics = sortTopics(topics.keySet());
    for (String topicName : sortedTopics) {
      Html topicRow = renderTopicRow(topicName, topics.get(topicName)); 
      div.add(topicRow);
    }
    return div;
  }
  
  private Html renderTopicRow(String topicName, List<PartitionMeta> pmeta) {
    Html row = new Html("div")
        .setAttr("class", "topicrow");
    
    StringBuilder sb = new StringBuilder(200);
    sb.append(ShowTopicContent.URL).append('?');
    ShowTopicContent.pTopic.appendToUrl(sb, topicName);
    ShowTopicContent.pOffset.appendToUrl(sb, -5);

    row.add("div")
    .add("a")
    .setAttr("href",  sb.toString())
    .addText(topicName);
    row.add("div").addText(""+pmeta.size());
    Html offsets = new Html("div");
    
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
