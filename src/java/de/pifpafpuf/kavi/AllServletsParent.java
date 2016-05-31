package de.pifpafpuf.kavi;

import java.io.IOException;
import java.io.Writer;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import de.pifpafpuf.web.html.HtmlPage;

public class AllServletsParent extends HttpServlet {
  private static final Logger log = KafkaViewerServer.getLogger();
  
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
    //page.addJs("vecovi.js");
    page.addCss("style.css");
    
    return page;
  }
  /*+******************************************************************/

}
