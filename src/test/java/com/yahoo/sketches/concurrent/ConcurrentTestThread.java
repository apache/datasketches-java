package com.yahoo.sketches.concurrent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author eshcar
 */
public abstract class ConcurrentTestThread extends Thread{

  private static final Log LOG = LogFactory.getLog(ConcurrentTestThread.class);

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

  public void run() {
    int num = 1;

    switch (type_) {
    case WRITER:
    	num = 10000000;
    	break;
    case READER:
    	num = 10000;
    	break;
    case MIXED:
    	num = 100000;
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
