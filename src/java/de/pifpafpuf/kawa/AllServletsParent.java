package de.pifpafpuf.kawa;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import de.pifpafpuf.web.html.EmptyElem;
import de.pifpafpuf.web.html.Html;
import de.pifpafpuf.web.html.HtmlPage;
import de.pifpafpuf.web.urlparam.UrlParamCodec;

public class AllServletsParent extends HttpServlet {
  private static final Logger log = KafkaWatcherServer.getLogger();
  private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss'Z'";
  DateTimeFormatter df = DateTimeFormatter.ofPattern(DATE_FORMAT);


  void sendPage(HttpServletResponse resp, HtmlPage page) {
    page.addContent(renderFooter());
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
    //page.addJs("jquery-3.1.0.slim.min.js");
    //page.addJs("kafka-watcher.js");

    page.addContent(renderNavi());
    return page;
  }
  /*+******************************************************************/
  private EmptyElem renderFooter() {
    Html footer = new Html("div")
        .setAttr("class", "footer")
        .addText(KafkaWatcherServer.getVersion());
    footer.add("a")
    .setAttr("href", "https://github.com/HaraldKi/kafka-watcher")
    .setAttr("target", "_blank")
    .addText("source on GitHub");
    return footer;
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

    Html status = div.add("div").setAttr("class", "uistatus");

    String tstamp  = dateFormat(System.currentTimeMillis());
    status.add("div")
    .setAttr("class", "pagetstamp")
    .addText(tstamp);

    status.add("div")
    .setAttr("class", "kafkaaddr")
    .addText(KafkaWatcherServer.getKafka())
    ;
    return div;
  }
  /*+******************************************************************/
  protected final Locale getLocale(HttpServletRequest req) {
    // more fancy stuff with cookies set by the user may come later
    return req.getLocale();
  }
  /*+******************************************************************/
  protected final String localeFormatLong(Locale l, long num) {
    return String.format(l, "%,d", num);
  }
  /*+******************************************************************/
  protected final String dateFormat(long timestamp) {
    ZonedDateTime d = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                                              ZoneId.of("UTC"));
    return df.format(d);
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
  /*+******************************************************************/
  protected final Html renderProblem(Throwable ex) {
    Html div = new Html("div").setAttr("class", "error");
    div.add("p")
    .addText("problems attaching to Kafka:")
    .add("b").addText(ex.getMessage());
    div.add("pre")
    .setAttr("class", "stacktrace")
    .addText(stacktrace(ex));
    return div;
  }
  /*+******************************************************************/
  private final String stacktrace(Throwable t) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream p;
    try {
      p = new PrintStream(out, false, "UTF-8");
      t.printStackTrace(p);
      return out.toString("UTF-8");
    } catch (UnsupportedEncodingException e) {
      return "this should never happen, but it seems we do not have UTF-8";
    }
  }
}
