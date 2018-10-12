package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.concurrent.ConcurrentTestContext;
import com.yahoo.sketches.concurrent.ConcurrentTestThread;

/**
 * @author eshcar
 * @author Lee Rhodes
 */
public class ConcurrentThetaTest {
  public  final Logger LOG = LoggerFactory.getLogger(ConcurrentThetaTest.class);
  enum CONCURRENCY_TYPE {CONCURRENT, BASELINE, LOCK_BASED}

  //Builder
  private ConcurrentThetaBuilder builder;

  // common parameters for local and shared objects
  private final long seed = DEFAULT_UPDATE_SEED; //default

  //Parameters for local buffer
  private final int local_lgK = 4;   //default
  private final int cacheLimit = 0;  //default

  private final boolean propagateCompact = true; //default
  private final boolean offHeap = false; //default
  private final boolean warmUp = false;


  //Shared Sketch
  private final int shared_lgK = 12; //default
  private final int poolThreads = 3; //default
  private WritableDirectHandle wdh = null;
  private WritableMemory wmem = null;
  private SharedThetaSketch sharedSketch;
  private UpdateSketch updateSketch;

  private CONCURRENCY_TYPE type = CONCURRENCY_TYPE.CONCURRENT;
  private int numWriterThreads = 4;
  private int numReaderThreads = 4;
  private int readerQueries = 10_000;
  private int writerUpdates = 10_000_000;
  private long baselineUpdates = 4_000_000_000L;
  //private int mixedNum = 100_000;
  private int timeToRun_S = 30;
  private long runTime_mS = 0;

  public ConcurrentThetaTest() throws Exception { //used with testNG
    this(CONCURRENCY_TYPE.CONCURRENT, 4, 4, 3);
  }

  public ConcurrentThetaTest(
      final CONCURRENCY_TYPE type,
      final int numWriterThreads,
      final int numReaderThreads,
      final int timeToRun_S) throws Exception {

    this.type = type;
    this.numWriterThreads = numWriterThreads;
    this.numReaderThreads = numReaderThreads;
    this.timeToRun_S = timeToRun_S;

    builder = configureBuilder(); //tmp

    setUp();

    if (type != CONCURRENCY_TYPE.BASELINE) {
      LOG.info("Time To Run    = " + timeToRun_S + " Seconds");
    }

    runTest();

    LOG.info("Done!");
  }

  @SuppressWarnings("unused")
  public static void main(String[] args) throws Exception {
    if(args.length < 4) {
      throw new IllegalArgumentException("Missing arguemnts: "
          + "java com.yahoo.sketches.concurrent.theta"
          + ".ConcurrentThetaTest <concurrencyType> <#writers> <#readers> <seconds> "
          + "\n e.g., java "
          + "com.yahoo.sketches.theta.TestPerformanceTheta CONCURRENT 4 4 30");
    }

    int i=0;
    CONCURRENCY_TYPE type = CONCURRENCY_TYPE.valueOf(args[i++]);
    int writers = Integer.parseInt(args[i++]);
    int readers = Integer.parseInt(args[i++]);
    int timeToRun_S = Integer.parseInt(args[i++]);

    new ConcurrentThetaTest(type, writers, readers, timeToRun_S);
  }

  public void setUp() throws Exception {
    UpdateSketch localSketch=null;

    final int maxSharedUpdateBytes = Sketch.getMaxUpdateSketchBytes(1 << shared_lgK);

    if(offHeap){
      wdh = WritableMemory.allocateDirect(maxSharedUpdateBytes);
      wmem = wdh.get();
    } else { //On-heap
      wmem = WritableMemory.allocate(maxSharedUpdateBytes);
    }
    //must build shared first
    sharedSketch = builder.setSharedLogNominalEntries(shared_lgK).build(wmem);

    switch (type) {

    case CONCURRENT:
      //sketch / gadget / sharedSketch already exists
      //sketchToInit
      System.out.println("");
      LOG.info("=============================================CONCURRENT_THETA"
          + "===========================================");
      LOG.info("Writer Threads = " + numWriterThreads);
      LOG.info("Reader Threads = " + numReaderThreads);
      break;
    case LOCK_BASED:
      updateSketch = new LockBasedUpdateSketch(shared_lgK,seed,wmem);
      //sketchToInit /localSketch already exists, cannot set from sharedSketch
      System.out.println("");
      LOG.info("=============================================LOCK_BASED_THETA"
          + "===========================================");
      LOG.info("Writer Threads = " + numWriterThreads);
      LOG.info("Reader Threads = " + numReaderThreads);
      break;
    case BASELINE:
      //sketch / gadget / sharedSketch already exists
      //sketchToinit /localSketch already exits, cannot set from sharedSketch
      UpdateSketch updateSketch = Sketches.updateSketchBuilder()
          .setNominalEntries(1 << shared_lgK).build(wmem);
      System.out.println("");
      LOG.info("=============================================BASELINE_THETA"
          + "===========================================");
      break;
    }
    if (warmUp) {
      localSketch = builder.build();
      StringBuilder sb = new StringBuilder();
      int num = 10_000_000;
      for (long i = 0; i < num; i++) { //Warm up
        localSketch.update(i);
        if((i % 100_000) == 0){
          sb.append(".");
        }
      }
      LOG.info(sb.toString());
      LOG.info("Total Writes = " + num);
      LOG.info("Estimate     = " + localSketch.getEstimate());
    }
  }

  private void runTest() throws Exception {
    long totalReads = 0;
    long totalWrites = 0;

    if (type == CONCURRENCY_TYPE.BASELINE) {
      long num = baselineUpdates; //numWriterThreads * writerUpdates;
      long start_mS = System.currentTimeMillis();
      for (long i = 0; i < num; i++) {
        updateSketch.update(i);
      }
      runTime_mS = System.currentTimeMillis() - start_mS;
      double time_S = runTime_mS / 1000.0;
      LOG.info("Runtime Sec    + " + time_S);
      LOG.info("Total Writes   = " + num);
      LOG.info("WriteTput      = " + (long) (((num / time_S)) / 1000000) + " millions per second");
      double estimate = updateSketch.getEstimate();
      LOG.info("Estimate       = " + estimate);
      double re = (estimate / num) - 1.0;
      LOG.info("Relative Error = " + (re * 100.0) + "%");
      LOG.info("Theta = "+ updateSketch.getTheta());
      LOG.info("Count = "+ updateSketch.getRetainedEntries());
      return;
    }

    ConcurrentTestContext ctx = new ConcurrentTestContext();

    List<WriterThread> writersList = new ArrayList<>();
    for (int i = 0; i < numWriterThreads; i++) {
      WriterThread writer = new WriterThread(type, i, numWriterThreads);
      writersList.add(writer);
      ctx.addThread(writer);
    }

    List<ReaderThread> readersList = new ArrayList<>();
    for (int i = 0; i < numReaderThreads; i++) {
      ReaderThread reader = new ReaderThread(type);
      readersList.add(reader);
      ctx.addThread(reader);
    }

    ctx.startThreads();
    ctx.waitFor(timeToRun_S * 1000);
    ctx.stop();

    if (type== CONCURRENCY_TYPE.CONCURRENT) {
      while(sharedSketch.isPropagationInProgress()) {}
    }

    for (WriterThread writer : writersList) {
      totalWrites += writer.operationsNum_;
    }
    LOG.info("Total Writes   = " + totalWrites);
    LOG.info("WriteTput      = " + (((totalWrites / timeToRun_S)) / 1000000)
        + " millions per second");

    for (ReaderThread reader : readersList) {
    	totalReads += reader.readOperationsNum_;
    }
    LOG.info("Total Reads    = " + totalReads);
    LOG.info("ReadTput       = " + (((totalReads / timeToRun_S)) / 1000000)
        + " millions per second");

    double estimate = -1;
    if (type== CONCURRENCY_TYPE.CONCURRENT) {
      estimate = sharedSketch.getEstimationSnapshot();
    } else {
      estimate = updateSketch.getEstimate();
    }
    LOG.info("Estimate       = " + estimate);
    double re = (estimate / totalWrites) - 1.0;
    LOG.info("Relative Error = " + (re * 100.0) + "%");

    if (wdh != null) { wdh.close(); }
  }

  public class WriterThread extends ConcurrentTestThread {
    long operationsNum_ = 0;
    private UpdateSketch context_;
    long i_;
    int jump_;

    public WriterThread(CONCURRENCY_TYPE type, int id) {
      this(type, id, 1);
    }

    public WriterThread(CONCURRENCY_TYPE type, int id, int jump) {
      super(writerUpdates); //for Writer pass 10_000_000
      i_ = id;
      jump_ = jump;
      context_ = (type == CONCURRENCY_TYPE.CONCURRENT)
          ? builder.build()
          : updateSketch; //IS THIS RIGHT?
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
    double stupidSum = 0;
    private UpdateSketch context_;

    public ReaderThread(CONCURRENCY_TYPE type) {
      super(readerQueries); //for Reader pass 10_000

      context_ = (type == CONCURRENCY_TYPE.CONCURRENT)
          ? builder.build() //local sketch
          : updateSketch; // IS THIS RIGHT?
    }

    @Override
    public void doWork() throws Exception {
      stupidSum += context_.getEstimate();
      readOperationsNum_++;
    }
  }

  private static class LockBasedUpdateSketch extends DirectQuickSelectSketch {

    private ReentrantReadWriteLock lock_;

    protected LockBasedUpdateSketch(
        final int lgNomLongs,
        final long seed,
        final WritableMemory dstMem) {
      super(lgNomLongs,
          seed,
          1.0F, //p
          ResizeFactor.X1, //rf,
          null,
          dstMem,
          false);

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

  //configures builder for both local and shared
  ConcurrentThetaBuilder configureBuilder() { //temporary
    ConcurrentThetaBuilder bldr = new ConcurrentThetaBuilder();
    bldr.setSharedLogNominalEntries(shared_lgK);
    bldr.setLocalLogNominalEntries(local_lgK);
    bldr.setSeed(seed);
    bldr.setCacheLimit(cacheLimit);
    bldr.setPropagateOrderedCompact(propagateCompact);
    return bldr;
  }

}
