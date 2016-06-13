package de.pifpafpuf.kawa;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.pifpafpuf.kawa.offmeta.OffsetInfo;
import de.pifpafpuf.kawa.offmeta.OffsetMetaKey;
import de.pifpafpuf.kawa.offmeta.OffsetMsgValue;
import de.pifpafpuf.web.html.EmptyElem;
import de.pifpafpuf.web.html.Html;
import de.pifpafpuf.web.html.HtmlPage;
import de.pifpafpuf.web.urlparam.BooleanCodec;
import de.pifpafpuf.web.urlparam.IntegerCodec;
import de.pifpafpuf.web.urlparam.UrlParamCodec;

public class ShowConsumerOffsets  extends AllServletsParent {
  public static final String URL = "/offsets";
  public static final UrlParamCodec<Integer> pRefreshSecs =
      new UrlParamCodec<>("refreshsecs", 
                          new IntegerCodec(1, Integer.MAX_VALUE));
  public static final UrlParamCodec<Boolean> pShowDead =
      new UrlParamCodec<>("dead", BooleanCodec.INSTANCE);
  public static final UrlParamCodec<Boolean> pShowClosed=
      new UrlParamCodec<>("closed", BooleanCodec.INSTANCE);
         
  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) {
    HtmlPage page = initPage("consumer offsets");
    int refreshSecs = pRefreshSecs.fromFirst(req, -1);
    addRefreshMeta(page, refreshSecs);

    QueueWatcher qw = KafkaWatcherServer.getQueueWatcher();
    qw.rewindOffsets(2000);
    Map<String, OffsetInfo> offs = qw.getLastOffsets(200);

    boolean withClosed = pShowClosed.fromFirst(req, false);
    boolean withDead = pShowDead.fromFirst(req, false);
    
    page.addContent(renderRefresh(refreshSecs));
    page.addContent(renderHeader());
    page.addContent(renderForm(withClosed, withDead));
    page.addContent(renderTable(offs, withClosed, withDead));
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
  private Html renderForm(boolean withClosed, boolean withDead) {
    Html form = new Html("form")
    .setAttr("method", "GET")
    .setAttr("action", URL)
    .setAttr("class", "offsetsform");
    
    Html closedCheckLabel = form.add("label");
    EmptyElem closedCheck = closedCheckLabel
        .addEmpty("input")
        .setAttr("type", "checkbox");
    closedCheckLabel.addText("with closed");
    pShowClosed.setParam(closedCheck, true);
    if (withClosed) {
      closedCheck.setAttr("checked", "");
    }
    
    Html deadCheckLabel = form.add("label"); 
    EmptyElem deadCheck = deadCheckLabel
        .addEmpty("input")
        .setAttr("type", "checkbox");
    deadCheckLabel.addText("with dead");
    pShowDead.setParam(deadCheck, true);
    if (withDead) {
      deadCheck.setAttr("checked", "");
    }
    
    form.add("input")
    .setAttr("type", "submit")
    .setAttr("name", "submit")
    .setAttr("value", "get");
    
    return form;
  }
  /*+******************************************************************/
  private EmptyElem renderTable(Map<String,OffsetInfo> offs,
                                boolean withClosed, boolean withDead) {
    Html table = new Html("table").setAttr("class", "groupdata withdata");
    Html theadrow = table.add("thead").add("tr");
    theadrow.add("th").addText("committed (UTC)");
    theadrow.add("th").addText("group ID");
    theadrow.add("th").addText("topic");
    theadrow.add("th").addText("partition");
    theadrow.add("th").addText("offset");
    theadrow.add("th").addText("head");
    theadrow.add("th").addText("lag");
    theadrow.add("th").addText("expires (UTC)");
    theadrow.add("th").addText("closed");
    theadrow.add("th").addText("dead");

    Html tbody = table.add("tbody");

    List<OffsetInfo> infos = new ArrayList<>(offs.size());
    infos.addAll(offs.values());
    Collections.sort(infos, CommitstampSorter.INSTANCE);

    OffsetMetaKey previous = null;
    for (OffsetInfo oi : infos) {
      OffsetMsgValue ov = oi.getValue();
      if (!withClosed && oi.isClosed()) {
        continue;
      }
      if (!withDead && oi.isDead()) {
        continue;
      }
      OffsetMetaKey ok = oi.key;
      Html tr = new Html("tr");
      addSkip(tr, previous, ok);
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
      tr.add("td").addText(oi.isClosed() ? "✘" : "").setAttr("class", "cal");
      tr.add("td").addText(oi.isDead() ? "✘" : "").setAttr("class", "cal");
      tbody.add(tr);
      previous = ok;
    }
    return table;
  }

  private void addSkip(Html tr, OffsetMetaKey previous, OffsetMetaKey ok) {
    if (previous!=null
        && previous.topic.equals(ok.topic)
        && previous.group.equals(ok.group)) {
      return;
    }
    tr.setAttr("class", "wtopmargin");
  }

  private enum CommitstampSorter implements Comparator<OffsetInfo> {
    INSTANCE;

    @Override
    public int compare(OffsetInfo o1, OffsetInfo o2) {
      OffsetMetaKey ok1 = o1.key;
      OffsetMetaKey ok2 = o2.key;
      int r = ok1.group.compareTo(ok2.group);
      if (r!=0) {
        return r;
      }
      r = ok1.topic.compareTo(ok2.topic);
      if (r!=0) {
        return r;
      }
      if (ok1.partition<ok2.partition) {
        return -1;
      }
      if (ok1.partition>ok2.partition) {
        return 1;
      }

      OffsetMsgValue v1 = o1.getValue();
      OffsetMsgValue v2 = o2.getValue();
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
      return 0;
    }
  }
}
