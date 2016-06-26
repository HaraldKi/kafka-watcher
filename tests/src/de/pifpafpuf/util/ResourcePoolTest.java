package de.pifpafpuf.util;

import static org.junit.Assert.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ResourcePoolTest {
  //private ResourcePool<Reporter> rpool;
  private LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
  private ResourcePool<Reporter> rpool = null;
  /*+******************************************************************/
  @Before
  public void setup() {
    queue.clear();
  }
  @After
  public void teardown() {
    rpool.close();
  }
  /*+******************************************************************/
  @Test
  public void basicTest() throws Exception {
    long expireFrequency = 200;
    rpool =  new ResourcePool<>(() -> new Reporter(), expireFrequency);
    Runner r = new Runner(rpool, queue);
    new Thread(r).start();

    queue.take();
    Thread.sleep(2*expireFrequency);
    assertEquals(0, r.rep.getClosed());
    r.endThread();
    Thread.sleep(2*expireFrequency);
    assertEquals(1, r.rep.getClosed());
  }
  /*+******************************************************************/
  @Test
  public void requeueTest() throws Exception {
    long expireFrequency = 50;
    rpool = new ResourcePool<>(() -> new Reporter(), expireFrequency);
    Runner r = new Runner(rpool, queue);
    new Thread(r).start();
    queue.take();
    assertEquals(0, r.rep.getClosed());
    Thread.sleep(10*expireFrequency); // give time to requeue often
    assertEquals(0, r.rep.getClosed());
    r.endThread();
    Thread.sleep(2*expireFrequency);
    assertEquals(1, r.rep.getClosed());
  }
  /*+******************************************************************/
  @Test
  public void bombOnLateGetTest() throws InterruptedException {
    rpool = new ResourcePool<>(() -> new Reporter(), 200);
    Runner rFine = new Runner(rpool, queue);
    new Thread(rFine).start();
    queue.take();
    rpool.close();
    rFine.endThread();
    assertEquals(rFine.rep, rFine.rep2);
    try {
      rpool.get();
      fail("this should have thrown an exception");
    } catch (IllegalStateException e) {
      // all nice and shiny
    }
  }
  /*+******************************************************************/
  @Test
  public void closeTest() throws Exception {
    rpool = new ResourcePool<>(() -> new Reporter(), 200);
    final int NUM = 20;
    List<Runner> l = new ArrayList<>(NUM);
    for (int i=0; i<NUM; i++) {
      Runner r = new Runner(rpool, queue);
      new Thread(r).start();
      l.add(r);
    }
    for (int i=0; i<NUM; i++) {
      queue.take();
    }
    rpool.close();
    for (int i=0; i<NUM; i++) {
      Runner r = l.get(i);
      r.endThread();
    }
    Thread.sleep(200);
    for (int i=0; i<NUM; i++) {
      Runner r = l.get(i);
      assertEquals(1, r.rep.getClosed());
    }
  }
  /*+******************************************************************/
  private static final class Runner implements Runnable {
    public volatile Reporter rep = null;
    public volatile Reporter rep2 = null;
    private final ResourcePool<Reporter> rpool;
    private final Queue<Object> q;
    private Thread t = null;
    private Semaphore sem = new Semaphore(0);

    public Runner(ResourcePool<Reporter> rpool, Queue<Object> q) {
      this.rpool = rpool;
      this.q = q;
    }
    public void endThread() throws InterruptedException {
      sem.release();
      t.join();
    }
    @Override
    public void run() {
      this.t = Thread.currentThread();
      rep = rpool.get();
      q.add(rep);
      try {
        sem.acquire();
      } catch (InterruptedException e) {
        throw new IllegalStateException("unexpected interrupt");
      }
      rep2 = rpool.get();
    }
  }
  /*+******************************************************************/
  private static final class Reporter implements Closeable {
    private volatile int closed = 0;
    @Override
    public void close() throws IOException {
      closed += 1;
    }
    public int getClosed() {
      return closed;
    }

  }
}
