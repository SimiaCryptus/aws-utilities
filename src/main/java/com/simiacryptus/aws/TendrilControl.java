/*
 * Copyright (c) 2019 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.simiacryptus.aws;

import com.esotericsoftware.kryonet.rmi.TimeoutException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.simiacryptus.lang.SerializableCallable;
import com.simiacryptus.lang.UncheckedSupplier;
import com.simiacryptus.ref.wrappers.RefString;
import com.simiacryptus.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TendrilControl implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TendrilControl.class);
  private final static HashMap<String, Promise> currentOperations = new HashMap<String, Promise>();
  private final static ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder().setDaemon(true).build());
  private final Tendril.TendrilLink inner;

  public TendrilControl(final Tendril.TendrilLink inner) {
    this.inner = inner;
  }

  public long time() {
    return inner.time();
  }

  public <T> T eval(final SerializableCallable<T> task) throws Exception {
    assert inner.isAlive();
    return inner.run(task);
  }

  @Nullable
  public <T> Future<T> start(UncheckedSupplier<T> task) {
    return start(task, 10, UUID.randomUUID().toString());
  }

  @Nullable
  public <T> Future<T> start(@Nullable UncheckedSupplier<T> task, int retries, String key) {
    if (null == task)
      return null;
    assert inner.isAlive();
    try {
      String taskKey = inner.run(() -> {
        Promise<T> promise = new Promise<>();
        boolean run;
        synchronized (currentOperations) {
          if (!currentOperations.containsKey(key)) {
            currentOperations.put(key, promise);
            run = true;
          } else {
            run = false;
          }
        }
        if (run)
          new Thread(() -> {
            try {
              logger.warn(RefString.format("Task Start: %s", key));
              promise.set(task.get());
            } catch (Throwable e) {
              logger.warn("Task Error", e);
              promise.set(e);
            } finally {
              logger.warn("Task Exit: " + key);
            }
          }).start();
        return key;
      });
      logger.info("Started task: " + taskKey, new RuntimeException());
      Promise<T> localPromise = new Promise<>();
      scheduledExecutorService.schedule(new PollerTask<>(taskKey, localPromise), 10, TimeUnit.SECONDS);
      return localPromise;
    } catch (Throwable e) {
      if (retries > 0) {
        logger.warn("Error starting " + task, e);
        return start(task, retries - 1, key);
      } else {
        throw Util.throwException(e);
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
    private final int maxRetries = 4;
    int failures = 0;

    public PollerTask(String taskKey, Promise<T> localPromise) {
      this.taskKey = taskKey;
      this.localPromise = localPromise;
    }

    @Override
    public void run() {
      try {
        Object result = inner.run(() -> {
          try {
            Promise promise = currentOperations.get(taskKey);
            assert promise != null;
            if (promise.isDone()) {
              logger.info(RefString.format("Task complete: %s", taskKey));
              return promise.get();
            } else {
              logger.info(RefString.format("Task running: %s", taskKey));
              return null;
            }
          } catch (Throwable e) {
            logger.info(RefString.format("Task error: %s", taskKey));
            return e;
          }
        });
        failures = 0;
        if (null != result) {
          if (result instanceof Throwable) {
            localPromise.set((Throwable) result);
          } else {
            localPromise.set((T) result);
          }
        } else {
          scheduledExecutorService.schedule(PollerTask.this, 5, TimeUnit.SECONDS);
        }
      } catch (TimeoutException e) {
        if (failures < maxRetries) {
          int newTimeout = (int) (15 * Math.pow(2, ++failures));
          logger.info(RefString.format("Error polling task; %s failures - Retry in %s seconds", failures, newTimeout), e);
          scheduledExecutorService.schedule(PollerTask.this, newTimeout, TimeUnit.SECONDS);
        } else {
          logger.warn(RefString.format("Error polling task; %s failures", failures), e);
          localPromise.set(e);
          throw Util.throwException(e);
        }
      } catch (Exception e) {
        logger.warn(RefString.format("Fatal error polling task after %s failures", failures), e);
        localPromise.set(e);
        throw Util.throwException(e);
      }
    }
  }
}
