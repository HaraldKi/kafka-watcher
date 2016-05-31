package de.pifpafpuf.kavi;

import javax.servlet.Servlet;

import org.apache.log4j.Logger;
import org.eclipse.jetty.rewrite.handler.RedirectRegexRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

public class KafkaViewerServer {
  private static final int TWO_HOURS_SECONDS = 2*3600;
  private static final Logger log = getLogger();
  private static Server server;

  // application status
  private static QueueWatcher qw = new QueueWatcher("localhost", 9092);
  
  /*+******************************************************************/
  public static void main(String[] argv) throws Exception {

    setupServer(argv);
    Runtime.getRuntime().addShutdownHook(new Stopper());
    server.join();
    log.info("server properly shut down");
  }
  /*+******************************************************************/
  public static QueueWatcher getQueueWatcher() {
    return qw;
  }
  /*+******************************************************************/
  private static void setupServer(String[] argv) throws Exception {
    log.info("starting server setup");
    System.setProperty("org.eclipse.jetty.util.log.class",
                       "de.pifpafpuf.vecovi.JettyLog4jLogging");

    QueuedThreadPool threadPool = new QueuedThreadPool(4, 2);
    server = new Server(threadPool);

    ServerConnector http = new ServerConnector(server);
    http.setPort(7100);
    server.addConnector(http);

    ServletContextHandler scContext =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    scContext.setContextPath("/");

    addServlet(scContext, ShowFileServlet.class);
    addServlet(scContext, ShowTopics.class);
    addServlet(scContext, ShowTopicContent.class);
    addServlet(scContext, ShowConsumerOffsets.class);

    // add resource handler for static files
    ResourceHandler fileHandler = new ResourceHandler();
    fileHandler.setCacheControl("public, max-age="+TWO_HOURS_SECONDS);
    fileHandler.setDirectoriesListed(false);
    fileHandler.setResourceBase("static");

    // note: order matters. If the fileHandler does not find the stuff, the
    // next handler may interfere, it seems. On the other hand, the
    // ServletContextHandler does not seem to let through any unmatched
    // requests.
    HandlerList handlers = new HandlerList();
    handlers.addHandler(fileHandler);
    handlers.addHandler(scContext);
    Handler rootRedir = rewriteRootHander(handlers);
    
    server.setHandler(rootRedir);
    server.start();
    log.info("Server now set up");
  }
  /*+******************************************************************/
  private static final void
  addServlet(ServletContextHandler ctx, Class<?> cls)
      throws IllegalArgumentException, IllegalAccessException,
      NoSuchFieldException, SecurityException
  {
    Object url = cls.getField("URL").get(null);
    @SuppressWarnings("unchecked")
    Class<Servlet> tcls = (Class<Servlet>)cls;
    ctx.addServlet(tcls, url.toString());
  }
  /*+******************************************************************/
  private static RewriteHandler rewriteRootHander(Handler client) {
    RewriteHandler rewrite = new RewriteHandler();
    rewrite.setHandler(client);

    // REMINDER: a pattern rule for '/' does not work, since it leads into a
    // redirect loop
    RedirectRegexRule r = new RedirectRegexRule();
    r.setRegex("^/?$");
    r.setReplacement(ShowTopics.URL);
    rewrite.addRule(r);

    return rewrite;
  }
  /*+******************************************************************/
  private static final class Stopper extends Thread {
    @Override
    public void run() {
      try {
        log.info("asking server to stop");
        server.stop();
      } catch (Exception e) {
        log.error("stopping the server did not really go well", e);
      }
    }
  }
 /*+******************************************************************/
  public static Logger getLogger() {
    StackTraceElement[] stack =  Thread.currentThread().getStackTrace();
    return Logger.getLogger(stack[2].getClassName());
  }
}
