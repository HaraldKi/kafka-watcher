package de.pifpafpuf.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/**
 * <p>
 * maintains a pool of resources in <code>ThreadLocal</code> variables and
 * closes the resouces once it detects that the using thread has terminated.
 * </p>
 * 
 * @param <T> is the type of resource to manage.
 */
public class ResourcePool<T extends Closeable> implements Closeable {
  private final Supplier<T> factory;
  private final LinkedBlockingQueue<ThreadAssoc<T>> queue =
      new LinkedBlockingQueue<>();
  private final Timer timer;
  private volatile long expireFrequencyMillis;
  private volatile boolean closed;

  private final ThreadLocal<ThreadAssoc<T>>
  resourceHolder = new ThreadLocal<ThreadAssoc<T>>() {
    @Override
    protected ThreadAssoc<T> initialValue() {
      // synchronize with timer.cancel() in TimerTask
      synchronized(ResourcePool.this) { 
        if (closed) {
          throw new IllegalStateException("ResourcePool already closed, cannot "
              + "hand out newly created resources");
        }
        ThreadAssoc<T> result =
            new ThreadAssoc<T>(Thread.currentThread(), factory.get());
        requeue(result);
        return result;
      }
    }
  };
  /*+******************************************************************/
  
  /**
   * creates the pool.
   * 
   * @param factory is used to create resources, one per thread and held in a
   *        <code>ThreadLocal</code> once created.
   * @param expireFrequencyMillis is the frequency with which it is checked
   *        whether threads have terminated. Can be set to a CPU friendly
   *        value of several seconds if not minutes, depending on how threads
   *        are expected to be created and terminated.
   */
  public ResourcePool(Supplier<T> factory, long expireFrequencyMillis) {
    this.factory = factory;
    this.expireFrequencyMillis = expireFrequencyMillis;
    this.timer = new Timer("ResourcePool("+factory+")");
    this.timer.schedule(new Worker(),  expireFrequencyMillis);
  }

  private void requeue(ThreadAssoc<T> elem) {
    elem.setCheckpoint(expireFrequencyMillis);
    queue.add(elem);
  }

  /**
   * <p>
   * Marks this pool for shutdown. If {@link #get} is called afterwards, it
   * will thrown an exception. Further, the internal timer task checking for
   * client threads to have terminated will not be re-scheduled after the
   * last client thread indeed has terminated.
   * </p>
   * 
   * <p>
   * To speed up noticing terminated threads, the expire frequency provided
   * in the constructor will now be overriden to be just 2000ms (2 seconds)
   * </p>
   * 
   * <p>
   * This method does not block, but only sets an internal flag.
   * </p>
   */
  @Override
  public void close() {
    closed = true;
    expireFrequencyMillis = 2000;
    try {
      // yes, this installs a second self-repeating task, but yes, we want
      // to terminate as soon as possible.
      timer.schedule(new Worker(), 1);
    } catch (IllegalStateException e) {
      // the closed=true triggered a timer.cancel already, which is fine
    }
  }

  /**
   * <p>
   * provides a resource as created by the supplier provided to the
   * constructor. Internally the resource is stored in a
   * <code>ThreadLocal</code> meaning that within the same thread, the
   * resource will always be the same and in different threads it will be
   * different.
   * </p>
   * 
   * @return
   */
  public T get() {
    return resourceHolder.get().value;
  }

  private final class Worker extends TimerTask {
    @Override
    public void run() {
      ThreadAssoc<T> elem = null;
      while (null!=(elem=queue.peek()) && elem.checkpointDelay()<=0) {
        queue.remove();
        if (elem.t.isAlive()) {
          requeue(elem);
        } else {
          silentCloseElem(elem);
        }
      }
      long delay = elem==null ? expireFrequencyMillis : elem.checkpointDelay();
      synchronized(ResourcePool.this) {
        if (closed && queue.isEmpty()) {
          timer.cancel();
        } else {
          timer.schedule(new Worker(), delay);
        }
      }
    }
  }

  private void silentCloseElem(ThreadAssoc<T> elem) {
    try {
      elem.value.close();
    } catch (IOException e) {
      // well then, do nothing, we said we'll keep silent
    }
  }

  private static final class ThreadAssoc<T extends Closeable> {
    public final Thread t;
    public final T value;
    private volatile long checkpoint = 0L;
    public ThreadAssoc(Thread t, T value) {
      this.t = t;
      this.value = value;
    }
    void setCheckpoint(long fromNowMillis) {
      this.checkpoint = System.currentTimeMillis() + fromNowMillis;
    }
    long checkpointDelay() {
      return checkpoint - System.currentTimeMillis();
    }
  }
}
