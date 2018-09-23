package com.simiacryptus.aws;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class Promise<T> implements Future<T> {
  public final Semaphore onReady = new Semaphore(0);
  public final AtomicReference<T> result = new AtomicReference<T>();

  public void set(T obj) {
    result.set(obj);
    onReady.release();
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    if (onReady.tryAcquire()) {
      onReady.release();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public T get() throws InterruptedException {
    onReady.acquire();
    onReady.release();
    return result.get();
  }

  @Override
  public T get(long timeout, @NotNull TimeUnit unit) throws InterruptedException, TimeoutException {
    if (onReady.tryAcquire(timeout, unit)) {
      onReady.release();
      return result.get();
    } else {
      throw new TimeoutException();
    }
  }
}
