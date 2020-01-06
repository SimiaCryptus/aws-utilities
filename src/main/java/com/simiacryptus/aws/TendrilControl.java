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
import com.simiacryptus.lang.SerializableSupplier;
import com.simiacryptus.ref.lang.RefAware;
import com.simiacryptus.ref.wrappers.RefHashMap;
import com.simiacryptus.ref.wrappers.RefString;
import com.simiacryptus.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public @RefAware
class TendrilControl implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TendrilControl.class);
  private final static RefHashMap<String, Promise> currentOperations = new RefHashMap<String, Promise>();
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

  public <T> Future<T> start(SerializableSupplier<T> task) {
    return start(task, 10, UUID.randomUUID().toString());
  }

  public <T> Future<T> start(SerializableSupplier<T> task, int retries, String key) {
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
              if (null == promise)
                throw new AssertionError();
              logger.warn(RefString.format("Task Start: %s = %s", key, JsonUtil.toJson(task)));
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
      if (retries > 0) {
        logger.warn("Error starting " + task, e);
        return start(task, retries - 1, key);
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

  private @RefAware
  class PollerTask<T> implements Runnable {
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
      Object result;
      try {
        result = inner.run(() -> {
          try {
            Promise promise = currentOperations.get(taskKey);
            if (promise.isDone()) {
              Object o = promise.get();
              failures = 0;
              return o;
            } else {
              failures = 0;
              return null;
            }
          } catch (TimeoutException e) {
            return null;
          }
        });
      } catch (Throwable e) {
        if (failures < maxRetries) {
          logger.info(RefString.format("Error polling task; %s failures", failures), e);
          result = null;
        } else {
          logger.warn(RefString.format("Error polling task; %s failures", failures), e);
          throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        }
      }
      if (null != result) {
        failures = 0;
        localPromise.set((T) result);
      } else {
        scheduledExecutorService.schedule(PollerTask.this, (int) (15 * Math.pow(2, ++failures)), TimeUnit.SECONDS);
      }
    }
  }
}
