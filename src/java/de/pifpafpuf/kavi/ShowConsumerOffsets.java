package de.pifpafpuf.kavi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.pifpafpuf.kavi.offmeta.OffsetInfo;
import de.pifpafpuf.kavi.offmeta.OffsetMetaKey;
import de.pifpafpuf.kavi.offmeta.OffsetMsgValue;
import de.pifpafpuf.web.html.EmptyElem;
import de.pifpafpuf.web.html.Html;
import de.pifpafpuf.web.html.HtmlPage;
import de.pifpafpuf.web.urlparam.IntegerCodec;
import de.pifpafpuf.web.urlparam.UrlParamCodec;

public class ShowConsumerOffsets  extends AllServletsParent {
  public static final String URL = "/offsets";
  public static final UrlParamCodec<Integer> pRefreshSecs =
      new UrlParamCodec<>("refreshsecs",
                          new IntegerCodec(1, Integer.MAX_VALUE));
  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) {
    HtmlPage page = initPage("consumer offsets");
    int refreshSecs = pRefreshSecs.fromFirst(req, -1);
    addRefreshMeta(page, refreshSecs);

    QueueWatcher qw = KafkaViewerServer.getQueueWatcher();
    qw.rewindOffsets(100);
    Map<String, OffsetInfo> offs = qw.getLastOffsets(200);

    page.addContent(renderRefresh(refreshSecs));
    page.addContent(renderHeader());
    page.addContent(renderTable(offs));
    sendPage(resp, page);
  }
  /*+******************************************************************/
  private EmptyElem renderRefresh(int refreshSecs) {
    StringBuilder sb = new StringBuilder(200);
    sb.append(URL).append('?');
    return renderRefreshButton(refreshSecs, sb, pRefreshSecs);
  }
  /*+********************************************************** ********/
  private Html renderHeader() {
    Html div = new Html("div");
    div.add("h1").addText("Groups offset overview");
    return div;
  }
  /*+******************************************************************/
  private EmptyElem renderTable(Map<String,OffsetInfo> offs) {
    Html table = new Html("table").setAttr("class", "groupdata withdata");
    Html thead = table.add("thead");
    thead.add("th").addText("committed (UTC)");
    thead.add("th").addText("group ID");
    thead.add("th").addText("topic");
    thead.add("th").addText("partition");
    thead.add("th").addText("offset");
    thead.add("th").addText("head");
    thead.add("th").addText("lag");
    thead.add("th").addText("expires (UTC)");
    thead.add("th").addText("dead");

    Html tbody = table.add("tbody");

    List<OffsetInfo> infos = new ArrayList<>(offs.size());
    infos.addAll(offs.values());
    Collections.sort(infos, CommitstampSorter.INSTANCE);


    for (OffsetInfo oi : infos) {
      OffsetMetaKey ok = oi.key;
      OffsetMsgValue ov = oi.value;
      Html tr = new Html("tr");
      tr.add("td").addText(ov!=null ? dateFormat(ov.commitStamp) : "");
      tr.add("td").addText(ok.group);

      tr.add("td")
      .addText(ok.topic);

      tr.add("td")
      .addText(Integer.toString(ok.partition))
      .setAttr("class", "ral");

      tr.add("td")
      .addText(ov!=null ? Long.toString(ov.offset) : "")
      .setAttr("class", "ral")
      ;
      tr.add("td")
      .addText(Long.toString(oi.tip))
      .setAttr("class", "ral")
      ;
      tr.add("td")
      .addText(ov!=null ? Long.toString(oi.tip-ov.offset) : "")
      .setAttr("class", "ral")
      ;

      tr.add("td").addText(ov!=null ? dateFormat(ov.expiresStamp) : "");
      tr.add("td").addText(oi.dead ? "✘" : "").setAttr("class", "cal");
      tbody.add(tr);
    }
    return table;
  }

  private enum CommitstampSorter implements Comparator<OffsetInfo> {
    INSTANCE;

    @Override
    public int compare(OffsetInfo o1, OffsetInfo o2) {
      return -asc(o1, o2);
    }

    public int asc(OffsetInfo o1, OffsetInfo o2) {
      OffsetMsgValue v1 = o1.value;
      OffsetMsgValue v2 = o2.value;
      if (v1==null) {
        return v2==null ? 0 : -1;
      } else if (v2==null) {
        return 1;
      }
      if (v1.commitStamp<v2.commitStamp) {
        return -1;
      }
      if (v1.commitStamp>v2.commitStamp) {
        return 1;
      }
      OffsetMetaKey ok1 = o1.key;
      OffsetMetaKey ok2 = o2.key;
      if (ok1.partition<ok2.partition) {
        return -1;
      }
      if (ok1.partition>ok2.partition) {
        return 1;
      }
      return 0;
    }
  }
}
