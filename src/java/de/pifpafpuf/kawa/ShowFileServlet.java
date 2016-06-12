package de.pifpafpuf.kawa;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.pifpafpuf.web.html.Html;
import de.pifpafpuf.web.html.HtmlPage;

public class ShowFileServlet  extends AllServletsParent {
  public static final String URL = "/show";
  
  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) {
    
    HtmlPage page = initPage("show file");
    Html div = new Html("div");
    div.addText("cool, it works");
    page.addContent(div);
    sendPage(resp, page);
  }
}
