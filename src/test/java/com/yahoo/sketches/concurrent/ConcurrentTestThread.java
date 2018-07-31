package com.yahoo.sketches.concurrent;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author eshcar
 */
public abstract class ConcurrentTestThread extends Thread{

  private static final  Logger LOG = LoggerFactory.getLogger(ConcurrentTestThread.class);

  enum ThreadType {
    MIXED, WRITER, READER
  }

  // protected final TestContext ctx_;
  private AtomicBoolean stop_ = new AtomicBoolean(false);
  private AtomicBoolean start_ = new AtomicBoolean(false);
  ThreadType type_;

  public ConcurrentTestThread(String type) {
    type_ = ThreadType.valueOf(type);
  }

  @Override
  public void run() {
    int num = 1;

    switch (type_) {
    case WRITER:
      num = 10_000_000;
      break;
    case READER:
      num = 10_000;
      break;
    case MIXED:
      num = 100_000;
      break;
    default:
      assert (false);
      break;
    }

    while (!start_.get()) {}

    try {
      while (!stop_.get()) {  //TODO can impact performance!

        for (int i = 0; i < num; i++) {
          doWork();
        }
      }
    } catch (Throwable t) {
      LOG.info("catched RuntimeException: " + t);
    }
  }

  public void stopThread() {
    stop_.set(true);
  }

  public void startThread() {
    start_.set(true);
  }

  public abstract void doWork() throws Exception;
}
