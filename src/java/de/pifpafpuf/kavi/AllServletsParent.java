package de.pifpafpuf.kavi;

import java.io.IOException;
import java.io.Writer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import de.pifpafpuf.web.html.EmptyElem;
import de.pifpafpuf.web.html.Html;
import de.pifpafpuf.web.html.HtmlPage;
import de.pifpafpuf.web.urlparam.UrlParamCodec;

public class AllServletsParent extends HttpServlet {
  private static final Logger log = KafkaViewerServer.getLogger();

  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final DateTimeFormatter dtf =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  void sendPage(HttpServletResponse resp, HtmlPage page) {
    resp.setContentType("text/html");
    resp.setCharacterEncoding("UTF-8");

    try {
      Writer w = resp.getWriter();
      page.print(w);
    } catch (IOException e) {
      log.error("could not write response body", e);
    }
  }
  /*+******************************************************************/
  HtmlPage initPage(String title) {
    HtmlPage page = new HtmlPage(title);
    page.addCss("style.css");

    page.addContent(renderNavi());
    return page;
  }
  /*+******************************************************************/
  private EmptyElem renderNavi() {
    Html div = new Html("div").setAttr("class", "navbar");
    StringBuilder sb = new StringBuilder(300);
    sb.append(ShowTopics.URL);
    div.add("a")
    .setAttr("href", ShowTopics.URL)
    .addText("Topics");
    
    div.add("a")
    .setAttr("href", ShowConsumerOffsets.URL)
    .addText("Offsets")
    ;
    return div;
  }
  /*+******************************************************************/
  protected final String dateFormat(long timestamp) {
    Instant stamp = Instant.ofEpochMilli(timestamp);
    ZonedDateTime d = ZonedDateTime.ofInstant(stamp, UTC);
    return dtf.format(d);
  }
  /*+******************************************************************/
  protected final void addRefreshMeta(HtmlPage page, int refreshSecs) {
    if (refreshSecs>0) {
      EmptyElem meta = new EmptyElem("meta");
      meta.setAttr("http-equiv", "refresh");
      meta.setAttr("content", Integer.toString(refreshSecs));
      page.addHeadElem(meta);
    }
  }
  /*+******************************************************************/
  protected final Html 
  renderRefreshButton(int refreshSecs, StringBuilder sb,
                      UrlParamCodec<Integer> pRefreshSecs) 
  {
    final int newRefresh = 10;
    Html a = new Html("a");
    if (refreshSecs<0) {
      pRefreshSecs.appendToUrl(sb, newRefresh);
      a.setAttr("href", sb.toString());
      a.addText("start refresh every "+newRefresh+" seconds");
    } else {
      a.setAttr("href", sb.toString());
      a.addText("stop auto-refresh");
    }
    return a;
  }
}
