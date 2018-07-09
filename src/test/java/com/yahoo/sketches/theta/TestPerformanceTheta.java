package com.yahoo.sketches.theta;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.concurrent.ConcurrentTestContext;
import com.yahoo.sketches.concurrent.ConcurrentTestThread;

/**
 * @author eshcar
 */
public class TestPerformanceTheta {

  private enum CONCURRENCY_TYPE {CONCURRENT, BASELINE, LOCK_BASED}
  private static final String OFF_HEAP = "OFF-HEAP";
  private static final int K = 512;

  private UpdateSketch gadget_;
  public final Logger LOG = LoggerFactory.getLogger(TestPerformanceTheta.class);

  @Test //enable to allow running from TestNG manually
  public static void startTest() throws Exception {
    TestPerformanceTheta.main(new String[] {"CONCURRENT", "QUICKSELECT", "4", "4", "30", "true"});
  }

  public static void main(String[] args) throws Exception {

    TestPerformanceTheta test = new TestPerformanceTheta();

    if(args.length<6) {
      test.LOG.info("Missing arguemnts: java com.yahoo.sketches.concurrent.theta"
          + ".TestPerformanceTheta <concurrencyType> <family> <#writers> <#readers> <seconds> "
          + "<print>\n e.g., java "
          + "com.yahoo.sketches.theta.TestPerformanceTheta CONCURRENT QUICKSELECT 4 4 "
          + "30 true");
      System.exit(0);
    }

    int i=0;
    String concurrencyType = args[i++];
    String family = args[i++];
    int writers = Integer.parseInt(args[i++]);
    int readers = Integer.parseInt(args[i++]);
    int time = Integer.parseInt(args[i++]);
    boolean print = Boolean.parseBoolean(args[i++]);

    if (print) {
      test.LOG.info("writers = " + writers);
    }

    test.setUp(concurrencyType, family);
    test.runTest(CONCURRENCY_TYPE.valueOf(concurrencyType), writers, readers, time);

    test.LOG.info("Done!");

    System.exit(0);
  }

  public void setUp(String concurrencyType, String family) throws Exception {

    WritableMemory mem;
    UpdateSketch sketch;
    UpdateSketch sketchToInit=null;
    UpdateSketchBuilder usb = Sketches.updateSketchBuilder().setNominalEntries(K);
    if(family.equalsIgnoreCase(OFF_HEAP)){
      mem = WritableMemory.wrap(new byte[(K * 16) + 24]);
      sketch = usb.build(mem);
    } else {
      sketch = usb.setFamily(Family.valueOf(family)).build();
    }

    switch (CONCURRENCY_TYPE.valueOf(concurrencyType)) {

    case CONCURRENT:
      if(family.equalsIgnoreCase(OFF_HEAP)){
        gadget_ = ConcurrentThetaFactory.createConcurrentUpdateSketch(sketch);
      } else {
        gadget_ = ConcurrentThetaFactory.createConcurrentUpdateSketch(Family.valueOf(family));
      }
      sketchToInit = ConcurrentThetaFactory.createConcurrentThetaContext(
          (ConcurrentUpdateSketch) gadget_);
      LOG.info("=============================================CONCURRENT_THETA"
          + "===========================================");
      break;
    case LOCK_BASED:
      gadget_ = new LockBasedUpdateSketch(sketch);
      sketchToInit = gadget_;
      LOG.info("=============================================LOCK_BASED_THETA"
          + "===========================================");
      break;
    case BASELINE:
      gadget_ = sketch;
      sketchToInit = sketch;
      LOG.info("=============================================BASELINE_THETA"
          + "===========================================");
      break;
    default:
      String msg = concurrencyType + "is not a valid concurrency type, please choose "
          + "CONCURRENT/LOCK_BASED/BASELINE";
      LOG.info(msg);
      throw new RuntimeException(msg);
    }

    for (long i = 0; i < 10000000; i++) {
      sketchToInit.update(i);
//      if((i % 100000) == 0){
//        System.out.print(".");
//      }
    }
    System.out.println();
  }

  private void runTest(CONCURRENCY_TYPE type, int writersNum, int readersNum, int secondsToRun)
      throws Exception {
    LOG.info("start running");
    ConcurrentTestContext ctx = new ConcurrentTestContext();

    List<WriterThread> writersList = new ArrayList<>();
    for (int i = 0; i < writersNum; i++) {
      WriterThread writer = new WriterThread(type, i, writersNum);
      writersList.add(writer);
      ctx.addThread(writer);
    }

    List<ReaderThread> readersList = new ArrayList<>();
    for (int i = 0; i < readersNum; i++) {
      ReaderThread reader = new ReaderThread(type);
      readersList.add(reader);
      ctx.addThread(reader);
    }

    ctx.startThreads();
    ctx.waitFor(secondsToRun * 1000);
    ctx.stop();

    long totalReads = 0;
    long totalWrites = 0;

    LOG.info("Write threads:");
    for (WriterThread writer : writersList) {
    	totalWrites += writer.operationsNum_;
    }
    LOG.info("writeTput = " + (((totalWrites / secondsToRun)) / 1000000) + " millions per second");

    LOG.info("Read threads:");
    for (ReaderThread reader : readersList) {
    	totalReads += reader.readOperationsNum_;
    }
    LOG.info("readTput = " + (((totalReads / secondsToRun)) / 1000000) + "millions per second");

    LOG.info("Estimation = " + gadget_.getEstimate());

  }

  public class WriterThread extends ConcurrentTestThread {
    long operationsNum_ = 0;
    private UpdateSketch context_;
    int i_;
    int jump_;

    public WriterThread(CONCURRENCY_TYPE type, int id) {
      this(type, id, 1);
    }

    public WriterThread(CONCURRENCY_TYPE type, int id, int jump) {
    	super("WRITER");
    	i_ = id;
    	jump_ = jump;
      switch (type) {
      case CONCURRENT:
        context_ = ConcurrentThetaFactory.createConcurrentThetaContext(
            (ConcurrentUpdateSketch) gadget_);
        break;
      default:
        context_ = gadget_;
      }
    }

    @Override
    public void doWork() throws Exception {

    	context_.update(i_);
    	operationsNum_++;
    	i_ = i_ + jump_;
    }
  }

  public class ReaderThread extends ConcurrentTestThread {

    long readOperationsNum_ = 0;
    private UpdateSketch context_;

    public ReaderThread(CONCURRENCY_TYPE type) {
      super("READER");
      switch (type) {
      case CONCURRENT:
        context_ = ConcurrentThetaFactory.createConcurrentThetaContext(
            (ConcurrentUpdateSketch) gadget_);
        break;
      default:
        context_ = gadget_;
      }

    }

    @Override
    public void doWork() throws Exception {

    	context_.getEstimate();
    	readOperationsNum_++;
    }
  }

  private static class LockBasedUpdateSketch extends UpdateSketchComposition {

    private ReentrantReadWriteLock lock_;

    protected LockBasedUpdateSketch(UpdateSketch delegatee) {
      super(delegatee);
      lock_ = new ReentrantReadWriteLock();
    }

    @Override public UpdateReturnState hashUpdate(long hash) {
      try {
        lock_.writeLock().lock();
        return super.hashUpdate(hash);
      } finally {
        lock_.writeLock().unlock();
      }

    }

    @Override public double getEstimate() {
      try {
        lock_.readLock().lock();
        return super.getEstimate();
      } finally {
        lock_.readLock().unlock();
      }

    }
  }
}
