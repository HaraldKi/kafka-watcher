package de.pifpafpuf.kawa;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.pifpafpuf.kawa.offmeta.GroupMsgValue;
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

    GroupStateWatcher gsw = KafkaWatcherServer.getGroupStateWatcher();
    Map<String, OffsetMsgValue> offs = gsw.getOffsetsState();
    Map<String, GroupMsgValue> groups = gsw.getGroupsState();

    boolean withClosed = pShowClosed.fromFirst(req, false);
    boolean withDead = pShowDead.fromFirst(req, false);

    page.addContent(renderRefresh(refreshSecs));
    page.addContent(renderHeader());
    page.addContent(renderForm(withClosed, withDead));
    page.addContent(renderTable(offs, groups, withClosed, withDead));

    page.addContent(renderGroups(groups, withClosed));
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
    closedCheckLabel.addText("closed groups");
    pShowClosed.setParam(closedCheck, true);
    if (withClosed) {
      closedCheck.setAttr("checked", "");
    }

    Html deadCheckLabel = form.add("label");
    EmptyElem deadCheck = deadCheckLabel
        .addEmpty("input")
        .setAttr("type", "checkbox");
    deadCheckLabel.addText("expired offsets");
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
  private EmptyElem renderTable(Map<String, OffsetMsgValue> offs,
                                Map<String, GroupMsgValue> groups,
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
    theadrow.add("th").addText("expired");

    Html tbody = table.add("tbody");

    List<OffsetMsgValue> infos = new ArrayList<>(offs.size());
    infos.addAll(offs.values());
    Collections.sort(infos, CommitstampSorter.INSTANCE);

    OffsetMetaKey previous = null;
    long topicLagTotal = 0;
    OffsetMetaKey latestOk = null;
    for (OffsetMsgValue oi : infos) {
      if (!withDead && oi.isExpired()) {
        continue;
      }
      OffsetMetaKey ok = oi.key;
      boolean groupClosed = groups.get(ok.group).isExpired();
      if (!withClosed && !withDead && groupClosed) {
        continue;
      }

      if (renderLagTotal(tbody, latestOk, ok.partition, topicLagTotal)) {
        topicLagTotal = 0;
      }
      latestOk = ok;

      Html tr = new Html("tr");
      addSkip(tr, previous, ok);
      tr.add("td").addText(dateFormat(oi.commitStamp));
      Html groupname = tr.add("td").addText(ok.group);
      if (groupClosed) {
        groupname.setAttr("class", "expired");
      }

      tr.add("td")
      .addText(ok.topic);

      tr.add("td")
      .addText(Integer.toString(ok.partition))
      .setAttr("class", "ral");

      tr.add("td")
      .addText(Long.toString(oi.offset))
      .setAttr("class", "ral")
      ;
      tr.add("td")
      .addText(oi.getHead()<0 ? "?" : Long.toString(oi.getHead()))
      .setAttr("class", "ral")
      ;
      long lag = oi.getHead()-oi.offset;
      topicLagTotal += lag;
      Html lagElem = tr.add("td")
          .addText(oi.getHead()<0 ? "" : Long.toString(lag));
      if (groupClosed) {
        lagElem .setAttr("class", "ral lagstuck");
      } else {
        lagElem .setAttr("class", "ral lag");
      }

      boolean expired = oi.isExpired();
      Html exp = tr.add("td").addText(dateFormat(oi.expiresStamp));
      if (expired) {
        exp.setAttr("class", "expired");
      }
      tr.add("td").addText(expired ? "✘" : "").setAttr("class", "cal");
      tbody.add(tr);
      previous = ok;
    }
    renderLagTotal(tbody, latestOk, 0, topicLagTotal);
    return table;
  }
  /*+******************************************************************/
  private boolean renderLagTotal(Html tbody,
                                 OffsetMetaKey ok, int currentPartition,
                                 long topicLagTotal) {
    if (ok==null || currentPartition>=ok.partition) {
      return false;
    }

    Html tr = new Html("tr");
    tr.add("td");
    tr.add("td").addText(ok.group);
    tr.add("td").addText(ok.topic);
    tr.add("td");
    tr.add("td");
    tr.add("td");
    tr.add("td")
    .setAttr("class", "ral")
    .addText(Long.toString(topicLagTotal));

    tr.add("td").addText("lag total");
    tr.add("td");

    tbody.add(tr);
    return true;
  }
  /*+******************************************************************/
  private void addSkip(Html tr, OffsetMetaKey previous, OffsetMetaKey ok) {
    if (previous!=null
        && previous.topic.equals(ok.topic)
        && previous.group.equals(ok.group)) {
      return;
    }
    tr.setAttr("class", "wtopmargin");
  }
  /*+******************************************************************/
  private Html renderGroups(Map<String, GroupMsgValue> groups,
                            boolean withClosed) {
    Html table = new Html("table").setAttr("class", "groupdata withdata");
    Html theadrow = table.add("thead").add("tr");
    theadrow.add("th").addText("group");
    theadrow.add("th").addText("proto type");
    theadrow.add("th").addText("gen ID");
    theadrow.add("th").addText("leader ID");
    theadrow.add("th").addText("protocol");
    theadrow.add("th").addText("members");
    theadrow.add("th").addText("closed");

    List<GroupMsgValue> infos = new ArrayList<>(groups.size());
    infos.addAll(groups.values());
    Html tbody = table.add("tbody");
    boolean first = true;
    for (GroupMsgValue gv : infos) {
      if (!withClosed && gv.isExpired()) {
        continue;
      }
      Html tr = tbody.add("tr");
      if (first) {
        tr.setAttr("class", "wtopmargin");
        first = false;
      }

      tr.add("td").addText(gv.key.group);
      tr.add("td").addText(gv.protocolType);
      tr.add("td").addText(Integer.toString(gv.generationId));
      tr.add("td").addText(gv.leaderId);
      tr.add("td").addText(gv.protocol);
      tr.add("td").addText(Integer.toString(gv.members.size()));
      tr.add("td").addText(gv.isExpired() ? "✘" : "").setAttr("class", "cal");
    }

    return table;
  }
  /*+******************************************************************/
  private enum CommitstampSorter implements Comparator<OffsetMsgValue> {
    INSTANCE;

    @Override
    public int compare(OffsetMsgValue o1, OffsetMsgValue o2) {
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

      OffsetMsgValue v1 = o1;
      OffsetMsgValue v2 = o2;
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
