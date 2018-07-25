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
  private int num;

  public ConcurrentTestThread(int num) {
    this.num = num;
  }

  @Override
  public void run() {

    while (!start_.get()) {} //wait for start

    try {
      while (!stop_.get()) {  //TODO can impact performance!
        for (int i = 0; i < num; i++) {
          doWork();
        }
      }
    } catch (Throwable t) {
      LOG.info("caught RuntimeException: " + t);
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
