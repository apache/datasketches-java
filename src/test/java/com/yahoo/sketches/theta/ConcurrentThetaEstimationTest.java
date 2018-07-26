package com.yahoo.sketches.theta;

import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static org.testng.Assert.assertTrue;

/**
 * @author eshcar
 */
public class ConcurrentThetaEstimationTest {
  public  final Logger LOG = LoggerFactory.getLogger(ConcurrentThetaEstimationTest.class);

  //Builder
  private ConcurrentThetaBuilder builder;

  // common parameters for local and shared objects
  private final long seed = DEFAULT_UPDATE_SEED; //default

  //Parameters for local buffer
  private final int local_lgK = 4;   //default
  private final int cacheLimit = 0;  //default

  private final boolean propagateCompact = true; //default


  //Shared Sketch
  private final int shared_lgK = 12; //default
  private final int poolThreads = 3; //default
  private WritableDirectHandle wdh = null;
  private WritableMemory wmem = null;
  private ConcurrentDirectThetaSketch sharedSketch;
  private ConcurrentHeapThetaBuffer localSketch;
  private UpdateSketch seqSketch;

  public ConcurrentThetaEstimationTest() throws Exception{
    builder = configureBuilder(); //tmp

    setUp();
    runTest();

    LOG.info("Done!");

  }

  public void setUp() throws Exception {

    final int maxSharedUpdateBytes = Sketch.getMaxUpdateSketchBytes(1 << shared_lgK);
    wmem = WritableMemory.allocate(maxSharedUpdateBytes);

    //must build shared first
    sharedSketch = builder.setSharedLogNominalEntries(shared_lgK).build(wmem);
    localSketch = builder.build();
    seqSketch = Sketches.updateSketchBuilder().setNominalEntries(1 << shared_lgK).build();
  }

  private void runTest() throws Exception {
    long num = 6_000_000_000L;
    for (long i = 1; i < num; i++) {
      localSketch.update(i);
      seqSketch.update(i);

      double seqEstimate = seqSketch.getEstimate();
      double sharedEstimate = sharedSketch.getEstimationSnapshot();
      if(sharedEstimate==i||sharedEstimate==i-1) continue;
      double error = sharedEstimate/i-1.0;
      if(error>0.05 || error<-0.05) {
        String s = "shared estimates error is greater than 5%: err=" + error
            + ", i=" + i
            + ", seq=" + seqEstimate
            + ", shared=" + sharedEstimate
            + ", seq theta="+seqSketch.getTheta()
            + ", seq counter="+seqSketch.getRetainedEntries()
            + ", shared theta="+sharedSketch.getTheta()
            + ", shared counter="+sharedSketch.getRetainedEntries();

        assertTrue(error < 0.05 && error > -0.05, s);
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
    bldr.setPoolThreads(poolThreads);
    return bldr;
  }

  public static void main(String[] args) throws Exception {
    new ConcurrentThetaEstimationTest();
  }
}
