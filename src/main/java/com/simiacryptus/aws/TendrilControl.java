package com.simiacryptus.aws;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.simiacryptus.lang.SerializableCallable;
import com.simiacryptus.lang.SerializableSupplier;
import com.simiacryptus.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * The type Tendril control.
 */
public class TendrilControl implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TendrilControl.class);
  private final static HashMap<String, Promise> currentOperations = new HashMap<String, Promise>();
  private final static ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build());
  private final Tendril.TendrilLink inner;

  /**
   * Instantiates a new Tendril control.
   *
   * @param inner the inner
   */
  public TendrilControl(final Tendril.TendrilLink inner) {
    this.inner = inner;
  }

  /**
   * Time long.
   *
   * @return the long
   */
  public long time() {
    return inner.time();
  }

  /**
   * Run t.
   *
   * @param <T>  the type parameter
   * @param task the task
   * @return the t
   * @throws Exception the exception
   */
  public <T> T eval(final SerializableCallable<T> task) throws Exception {
    assert inner.isAlive();
    return inner.run(task);
  }

  public <T> Future<T> start(SerializableSupplier<T> task) {
    return start(task, 10);
  }

  public <T> Future<T> start(SerializableSupplier<T> task, int retries) {
    if (null == task) return null;
    assert inner.isAlive();
    try {
      String taskKey = inner.run(() -> {
        Promise<T> promise = new Promise<>();
        String key = UUID.randomUUID().toString();
        currentOperations.put(key, promise);
        new Thread(() -> {
          try {
            if (null == promise) throw new AssertionError();
            logger.warn(String.format("Task Start: %s = %s", key, JsonUtil.toJson(task)));
            promise.set(task.get());
          } catch (Exception e) {
            logger.warn("Task Error", e);
          } finally {
            logger.warn("Task Exit: " + key);
          }
        }).start();
        return key;
      });
      Promise<T> localPromise = new Promise<>();
      scheduledExecutorService.schedule(new PollerTask<>(taskKey, localPromise), 10, TimeUnit.SECONDS);
      return localPromise;
    } catch (Throwable e) {
      if(retries >0){
        logger.warn("Error starting " + task,e);
        return start(task, retries-1);
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() {
    logger.info("Closing " + this);
    inner.exit();
  }

  private class PollerTask<T> implements Runnable {
    private final String taskKey;
    private final Promise<T> localPromise;

    public PollerTask(String taskKey, Promise<T> localPromise) {
      this.taskKey = taskKey;
      this.localPromise = localPromise;
    }

    @Override
    public void run() {
      poll(10);
    }

    private void poll(int retries) {
      try {
        Object result = inner.run(() -> {
          try {
            return currentOperations.get(taskKey).get(1, TimeUnit.SECONDS);
          } catch (TimeoutException e) {
            return null;
          }
        });
        if (null != result) {
          localPromise.set((T) result);
        } else {
          scheduledExecutorService.schedule(PollerTask.this, 10, TimeUnit.SECONDS);
        }
      } catch (Throwable e) {
        if (retries > 0) {
          logger.info("Error polling task", e);
          poll(retries - 1);
        } else {
          logger.warn("Error polling task", e);
          throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        }
      }
    }
  }
}
