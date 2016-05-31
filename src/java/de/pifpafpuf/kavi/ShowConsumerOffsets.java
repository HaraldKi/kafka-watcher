package de.pifpafpuf.kavi;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.pifpafpuf.kavi.offmeta.OffsetInfo;
import de.pifpafpuf.web.html.EmptyElem;
import de.pifpafpuf.web.html.Html;
import de.pifpafpuf.web.html.HtmlPage;

public class ShowConsumerOffsets  extends AllServletsParent {
  public static final String URL = "/offsets";
  
  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) {
    
    HtmlPage page = initPage("consumer offsets");
    QueueWatcher qw = KafkaViewerServer.getQueueWatcher();
    qw.rewindOffsets(10);
    Map<String, OffsetInfo> offs = qw.getLastOffsets(200);

    page.addContent(renderTable(offs));
    sendPage(resp, page);
  }

  private EmptyElem renderTable(Map<String,OffsetInfo> offs) {
    Html table = new Html("table");
    Html tbody = table.add("tbody");
    
    for (Map.Entry<String, OffsetInfo> elem : offs.entrySet()) {
      Html tr = new Html("tr");
      tr.add("td").addText(elem.getKey());
      tr.add("td").addText(elem.getValue().toString());
      tbody.add(tr);
    }
    return table;
  }
}
