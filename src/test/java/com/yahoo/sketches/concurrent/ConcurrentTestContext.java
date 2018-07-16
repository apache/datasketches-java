package com.yahoo.sketches.concurrent;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author eshcar
 */
public class ConcurrentTestContext {
  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentTestContext.class);

  private Throwable err_ = null;
  private Set<ConcurrentTestThread> testThreads_ = new HashSet<>();

  public void addThread(ConcurrentTestThread t) {
    testThreads_.add(t);
  }

  public void startThreads() {
    for (ConcurrentTestThread t : testThreads_) {
      t.start();
    }

    for (ConcurrentTestThread t : testThreads_) {
      t.startThread();
    }
  }

  private synchronized void checkException() throws Exception {
    if (err_ != null) {
      throw new RuntimeException("Deferred", err_);
    }
  }

  public synchronized void threadFailed(Throwable t) {
    if (err_ == null) {
      err_ = t;
    }
    LOG.error("Failed!", err_);
    notify();
  }

  public void stop() throws Exception {
    for (ConcurrentTestThread t : testThreads_) {
      t.stopThread();
    }

    for (ConcurrentTestThread t : testThreads_) {
      t.join();
    }
    checkException();
  }

  public void waitFor(long millis) throws Exception {
    long endTime = System.currentTimeMillis() + millis;
    while (true) {
      long left = endTime - System.currentTimeMillis();
      if (left <= 0) {
        break;
      }
    }
  }
}
