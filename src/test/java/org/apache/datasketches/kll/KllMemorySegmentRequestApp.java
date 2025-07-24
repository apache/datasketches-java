package org.apache.datasketches.kll;

import static org.apache.datasketches.kll.KllSketch.getMaxSerializedSizeBytes;
import static org.apache.datasketches.kll.KllSketch.SketchType.LONGS_SKETCH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.datasketches.common.MemorySegmentRequestExtension;
import org.apache.datasketches.common.MemorySegmentRequest;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllLongsSketch;
import org.testng.annotations.Test;


public class KllMemorySegmentRequestApp {

  @Test
  /**
   * This method emulates an application using an off-heap KLL sketch that needs to expand off-heap.
   * This demonstrates one example of how to manage a growing off-heap KLL sketch where the
   * expanded MemorySegments are also off-heap.
   */
  public void checkMemorySegmentRequestExtension() {
    final int k = 200;

    //The allocation of the original off-heap MemorySegment for the KllLongsSketch
    //Note that we target the size to only handle k values, which is quite small.
    final int numBytes = getMaxSerializedSizeBytes(k, k, LONGS_SKETCH, true);
    final Arena arena = Arena.ofConfined();
    final MemorySegment seg = arena.allocate(numBytes);

    //Our custom extension of the MemorySegmentRequest interface (see below).
    final MemorySegmentRequestExtension memSegReqExt = new MemorySegmentRequestExtension();

    //We create a new KllLongsSketch and pass our custom extension
    final KllLongsSketch sk = KllLongsSketch.newDirectInstance(k, seg, memSegReqExt);

    //We update the sketch with way more data than the original MemorySegment can handle, forcing it to expand.
    for (int n = 1; n <= (10 * k); n++) { sk.update(n); }

    //We make sure the sketch got all the data:
    assertEquals(sk.getMaxItem(), 10 * k);
    assertEquals(sk.getMinItem(), 1);
    assertEquals(sk.getN(), 10 * k);

    //We confirm that the last MemorySegment used by the sketch is, in fact, not the same as the original one we allocated.
    assertFalse(sk.getMemorySegment().equals(seg));

    //We are all done with the sketch. We cleanup any unclosed off-heap MemorySegments.
    memSegReqExt.cleanup();

    //We close the original off-heap allocated MemorySegment.
    arena.close(); println("Close original MemorySegment");
  }

  /**
   * Println Object o
   * @param o object to print
   */
  static void println(final Object o) {
    //System.out.println(o.toString());
  }

}
