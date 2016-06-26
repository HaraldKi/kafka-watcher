package de.pifpafpuf.kawa;

import java.util.concurrent.TimeUnit;

import javax.servlet.Servlet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

import de.pifpafpuf.util.ResourcePool;

public class KafkaWatcherServer {
  private static final int TWO_HOURS_SECONDS = 2*3600;
  private static final Logger log = getLogger();
  private static Server server;

  // application status
  private static ResourcePool<QueueWatcher> qWatchers;
  private static GroupStateWatcher gsw = null;
  private static String kafkahostport = null;
  /*+******************************************************************/
  public static void main(String[] argv) throws Exception {
    try {
      setupServer(argv);
      Runtime.getRuntime().addShutdownHook(new Stopper());
      if (server!=null) {
        server.join();
        log.info("server properly shut down");
      }
    } catch (Exception e) {
      if (server!=null) {
        server.stop();
      }
      if (qWatchers!=null) {
        qWatchers.close();
      }
      e.printStackTrace();
    }
  }
  /*+******************************************************************/
  public static QueueWatcher getQueueWatcher() {
    return qWatchers.get();
  }
  /*+******************************************************************/
  public static GroupStateWatcher getGroupStateWatcher() {
    return gsw;
  }
  /*+******************************************************************/
  public static String getKafka() {
    return kafkahostport;
  }
  /*+******************************************************************/
  private static void setupServer(String[] argv) throws Exception {
    CommandLine cli = parseCli(argv);
    if (cli==null) {
      return;
    }
    int port = -1;
    try {
      port = Integer.parseInt(cli.getOptionValue("p", "7311"));
    } catch (NumberFormatException e) {
      usage(createOptions(), "cannot parse as integer: `"
            +cli.getOptionValue("p")+"'");
      return;
    }
    
    kafkahostport = cli.getOptionValue("b", "localhost:9092");
    log.info("contacting kafka server at "+kafkahostport);    
    qWatchers = new ResourcePool<>(()->new QueueWatcher(kafkahostport), 
        TimeUnit.MINUTES.toMillis(1));
        
    gsw = new GroupStateWatcher(kafkahostport);
    new Thread(gsw, "GroupStateWatcher").start();
    
    System.setProperty("org.eclipse.jetty.util.log.class",
                       "de.pifpafpuf.vecovi.JettyLog4jLogging");

    QueuedThreadPool threadPool = new QueuedThreadPool(6, 2);

    log.info("starting server setup");
    server = new Server(threadPool);

    log.info("Starting server on port "+port);
    ServerConnector http = new ServerConnector(server);
    http.setPort(port);
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
        if (server!=null) {
          server.stop();
        }
      } catch (Exception e) {
        log.error("stopping the server did not really go well", e);
      }
      if (qWatchers!=null) {
        log.info("asking the kafka log watcher to stop");
        qWatchers.close();
      }
      if (gsw!=null) {
        log.info("asking the group state watcher to stop");
        gsw.shutdown();
      }
    }
  }
 /*+******************************************************************/
  public static Logger getLogger() {
    StackTraceElement[] stack =  Thread.currentThread().getStackTrace();
    return Logger.getLogger(stack[2].getClassName());
  }
  /*+******************************************************************/
  private static Options createOptions() {
    Options opts = new Options();
    Option hostport = 
        Option.builder("b")
        .longOpt("bootstrap-servers")
        .argName("hostport")
        .desc("bootstrap kafka servers in the same form as for the properties"
            + ", defaults to localhost:9092")
        .numberOfArgs(1)
        .build()
        ;
    opts.addOption(hostport);
    Option port =
        Option.builder("p")
        .argName("port")
        .desc("port on which to start the kafka-watcher, defaults to 7311")
        .numberOfArgs(1)
        .build();
    opts.addOption(port);
    return opts;
  }
  private static CommandLine parseCli(String[] argv) {
    Options opts = createOptions();
    try {
      return new DefaultParser().parse(opts, argv);
    } catch (ParseException e) {
      usage(opts, e.getMessage());
      return null;
    }
  }
  /*+******************************************************************/
  private static void usage(Options opts, String message) {
    HelpFormatter hf = new HelpFormatter();
    hf.printHelp(KafkaWatcherServer.class.getName(),
                 "    start server to watch a Kafka server", opts, "", true); 
    System.out.printf("%n%s%n", message);
  }
}
