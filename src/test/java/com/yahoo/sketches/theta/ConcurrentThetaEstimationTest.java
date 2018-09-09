package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;

/**
 * @author eshcar
 * @deprecated  This approach is not correct.  Please refer to the characterization repository.
 */
@Deprecated
public class ConcurrentThetaEstimationTest {
  public  final Logger LOG = LoggerFactory.getLogger(ConcurrentThetaEstimationTest.class);
  public static final String LS = System.getProperty("line.separator");

  //Builder
  private ConcurrentThetaBuilder builder;

  // common parameters for local and shared objects
  private final long seed = DEFAULT_UPDATE_SEED; //default

  //Parameters for local buffer
  private final int local_lgK = 4;   //default
  private final int cacheLimit = 0;  //default

  private final boolean propagateCompact = true; //default


  //Shared Sketch
  private final int sharedLgK = 12; //default
  private final int sharedK = 1 << sharedLgK;
  private final double rse = 1.0 / Math.sqrt(sharedK - 1);
  private final int poolThreads = 3; //default
  private WritableMemory wmem = null;
  private SharedThetaSketch sharedSketch;

  private ConcurrentHeapThetaBuffer localSketch;
  private UpdateSketch seqSketch;

  public ConcurrentThetaEstimationTest() {
    builder = configureBuilder();

    setUp();
    runTest();

    LOG.info("Done!");
  }

  public void setUp() {

    final int maxSharedUpdateBytes = Sketch.getMaxUpdateSketchBytes(sharedK);
    wmem = WritableMemory.allocate(maxSharedUpdateBytes); //on-heap

    //must build shared first
    sharedSketch = builder.build(wmem);
    localSketch = builder.build();
    seqSketch = Sketches.updateSketchBuilder().setNominalEntries(sharedK).build();
  }

  private void runTest()  {
    //long num = 6_000_000_000L;
    long num = 6_000_000L;
    double thresh = 2 * rse;
    System.err.println("Thresh = " + (thresh * 100) + "%");
    for (long i = 1; i <= num; i++) {
      localSketch.update(i);
      seqSketch.update(i);

      double shEst = sharedSketch.getEstimationSnapshot();

      if(shEst >= (i - 1)) { continue; }

      double shErr = (shEst / i) - 1.0;

      if ((shErr > thresh) || (shErr < -thresh)) {
        output(i, sharedSketch, seqSketch);
      }
    }
  }

  void output(long i, SharedThetaSketch shared, Sketch seq) {
    double sharedEstimate = shared.getEstimationSnapshot();
    double seqEstimate = seq.getEstimate();
    double shError = (sharedEstimate / i) - 1.0;
    double seqError = (seqEstimate / i) - 1.0;
//    int shEnt = shared.getRetainedEntries(true);
    int seqEnt = seq.getRetainedEntries(true);
//    double shTheta = shared.getTheta();
    double seqTheta = seq.getTheta();

    String s = "i=" + i + ", shared/seq: "
        + "est=" + sharedEstimate  + "/" + seqEstimate
        + ", error=" + shError + "/" + seqError;
//        + ", entries=" + shEnt + "/" + seqEnt
//        + ", theta="+shTheta + "/" + seqTheta;
//        + ", shared RSE=" + rse;
      System.err.println(s);
  }

    //configures builder for both local and shared
  ConcurrentThetaBuilder configureBuilder() {
    ConcurrentThetaBuilder bldr = new ConcurrentThetaBuilder();
    bldr.setSharedLogNominalEntries(sharedLgK);
    bldr.setLocalLogNominalEntries(local_lgK);
    bldr.setSeed(seed);
    bldr.setCacheLimit(cacheLimit);
    bldr.setPropagateOrderedCompact(propagateCompact);
    return bldr;
  }

  @Test
  public void startTest() throws Exception { }

  @SuppressWarnings("unused")
  public static void main(String[] args) throws Exception {
    new ConcurrentThetaEstimationTest();
  }
}
