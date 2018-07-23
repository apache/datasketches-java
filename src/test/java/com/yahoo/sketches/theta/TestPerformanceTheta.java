package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.concurrent.ConcurrentTestContext;
import com.yahoo.sketches.concurrent.ConcurrentTestThread;

/**
 * @author eshcar
 */
public class TestPerformanceTheta {

  private enum CONCURRENCY_TYPE {CONCURRENT, BASELINE, LOCK_BASED}
  // parameters for the concurrent classes
  private final int shared_lgK = 12;
  private final int local_lgK = 4;
  private final long seed = DEFAULT_UPDATE_SEED;
  private final int maxSharedUpdateBytes = Sketch.getMaxUpdateSketchBytes(1 << shared_lgK);
  private final int cacheLimit = 1;
  private final int poolThreads = 3;
  private final boolean propagateCompact = true;
  private final boolean offHeap = false;
  private final boolean warmUp = false;

  private WritableDirectHandle wdh = null;
  private WritableMemory wmem = null;
  private ConcurrentThetaBuilder builder;
  private UpdateSketch sharedSketch;

  //private CONCURRENCY_TYPE concurrencyType;


  public final Logger LOG = LoggerFactory.getLogger(TestPerformanceTheta.class);



  public static void main(String[] args) throws Exception {

    TestPerformanceTheta test = new TestPerformanceTheta();

    if(args.length<5) {
      test.LOG.info("Missing arguemnts: java com.yahoo.sketches.concurrent.theta"
          + ".TestPerformanceTheta <concurrencyType> <#writers> <#readers> <seconds> "
          + "<print>\n e.g., java "
          + "com.yahoo.sketches.theta.TestPerformanceTheta CONCURRENT 4 4 30 true");
      System.exit(0);
    }
    test.builder = test.configureBuilder();

    int i=0;
    CONCURRENCY_TYPE concurrencyType = CONCURRENCY_TYPE.valueOf(args[i++]);
    int writers = Integer.parseInt(args[i++]);
    int readers = Integer.parseInt(args[i++]);
    int time = Integer.parseInt(args[i++]);
    boolean print = Boolean.parseBoolean(args[i++]);

    if (print) {
      test.LOG.info("writers = " + writers);
      test.LOG.info("readers = " + readers);
    }

    test.setUp(concurrencyType);
    test.runTest(concurrencyType, writers, readers, time);

    test.LOG.info("Done!");

    System.exit(0);
  }

  public void setUp(CONCURRENCY_TYPE concurrencyType) throws Exception {
    UpdateSketch localSketch=null;

    if(offHeap){
      WritableDirectHandle wdh = WritableMemory.allocateDirect(maxSharedUpdateBytes);
      wmem = wdh.get();
    } else { //On-heap
      wmem = WritableMemory.allocate(maxSharedUpdateBytes);
    }

    sharedSketch = builder.build(wmem); //must build first

    switch (concurrencyType) {

    case CONCURRENT:
      //sketch / gadget / sharedSketch already exists
      //sketchToInit / localSketch already exists
      LOG.info("=============================================CONCURRENT_THETA"
          + "===========================================");
      break;
    case LOCK_BASED:
      sharedSketch = new LockBasedUpdateSketch(shared_lgK, seed, wmem, poolThreads);
      //sketchToInit /localSketch already exists, cannot set from sharedSketch
      LOG.info("=============================================LOCK_BASED_THETA"
          + "===========================================");
      break;
    case BASELINE:
      //sketch / gadget / sharedSketch already exists
      //sketchToinit /localSketch already exits, cannot set from sharedSketch

      LOG.info("=============================================BASELINE_THETA"
          + "===========================================");
      break;
    }
    if (warmUp) {
      localSketch = builder.build();
      StringBuilder sb = new StringBuilder();
      for (long i = 0; i < 10_000_000; i++) { //Warm up
        localSketch.update(i);
        if((i % 100_000) == 0){
          sb.append(".");
        }
      }
      LOG.info(sb.toString());
      LOG.info("Estimate: " + localSketch.getEstimate());
    }
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

    LOG.info("Write threads: " + writersList.size());
    for (WriterThread writer : writersList) {
      totalWrites += writer.operationsNum_;
    }
    LOG.info("writeTput = " + (((totalWrites / secondsToRun)) / 1000000) + " millions per second");

    LOG.info("Read threads: " + readersList.size());
    for (ReaderThread reader : readersList) {
    	totalReads += reader.readOperationsNum_;
    }
    LOG.info("readTput = " + (((totalReads / secondsToRun)) / 1000000) + " millions per second");

    LOG.info("Estimate = " + sharedSketch.getEstimate());

    if (wdh != null) { wdh.close(); }
  }

  public class WriterThread extends ConcurrentTestThread {
    long operationsNum_ = 0;
    private ConcurrentHeapThetaBuffer context_;
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
        context_ = builder.build();
        break;
      default:
        context_ = builder.build();
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
    private ConcurrentHeapThetaBuffer context_;

    public ReaderThread(CONCURRENCY_TYPE type) {
      super("READER");
      switch (type) {
      case CONCURRENT:
        context_ = builder.build();
        break;
      default:
        context_ = builder.build();
      }
    }

    @Override
    public void doWork() throws Exception {
      context_.getEstimate();
      readOperationsNum_++;
    }
  }

  private static class LockBasedUpdateSketch extends ConcurrentDirectThetaSketch {

    private ReentrantReadWriteLock lock_;

    protected LockBasedUpdateSketch(final int lgK, final long seed, final WritableMemory wmem,
        final int poolThreads) {
      super(lgK, seed, wmem, poolThreads);

      lock_ = new ReentrantReadWriteLock();
    }

    @Override
    UpdateReturnState hashUpdate(long hash) {
      try {
        lock_.writeLock().lock();
        return super.hashUpdate(hash);
      } finally {
        lock_.writeLock().unlock();
      }

    }

    @Override
    public double getEstimate() {
      try {
        lock_.readLock().lock();
        return super.getEstimate();
      } finally {
        lock_.readLock().unlock();
      }
    }
  }

  @Test //enable to allow running from TestNG manually
  public static void startTest() throws Exception {
    TestPerformanceTheta.main(new String[] {"CONCURRENT", "4", "4", "3", "false"});
  }

  //@Test //this is for sanity checking :)
  public void simpleSketchTest() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    for (int i = 0; i < 512; i++) { sk.update(i); }
    double est = sk.getEstimate();
    assertEquals(est, 512.0);
    System.out.println(sk.getEstimate());
  }

  //configures builder for both local and shared
  ConcurrentThetaBuilder configureBuilder() {
    ConcurrentThetaBuilder bldr = new ConcurrentThetaBuilder();
    bldr.setSharedNominalEntries(1 << shared_lgK);
    bldr.setSeed(seed);
    bldr.setCacheLimit(cacheLimit);
    bldr.setPropagateOrderedCompact(propagateCompact);
    bldr.setPoolThreads(poolThreads);
    return bldr;
  }

}
