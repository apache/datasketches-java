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
    //Note that this targets the size to only handle k values, which is quite small.
    final int numBytes = getMaxSerializedSizeBytes(k, k, LONGS_SKETCH, true);
    final Arena arena = Arena.ofConfined();
    final MemorySegment seg = arena.allocate(numBytes);

    //Use the custom extension of the MemorySegmentRequest interface.
    final MemorySegmentRequestExtension memSegReqExt = new MemorySegmentRequestExtension();

    //Create a new KllLongsSketch and pass the custom extension
    final KllLongsSketch sk = KllLongsSketch.newDirectInstance(k, seg, memSegReqExt);

    //Update the sketch with way more data than the original MemorySegment can handle, forcing it to request larger MemorySegments.
    for (int n = 1; n <= (10 * k); n++) { sk.update(n); }

    //Check to make sure the sketch got all the data:
    assertEquals(sk.getMaxItem(), 10 * k);
    assertEquals(sk.getMinItem(), 1);
    assertEquals(sk.getN(), 10 * k);

    //Confirm that the last MemorySegment used by the sketch is, in fact, not the same as the original one that was allocated.
    assertFalse(sk.getMemorySegment().equals(seg));

    //All done with the sketch. Cleanup any unclosed off-heap MemorySegments.
    memSegReqExt.cleanup();

    //Close the original off-heap allocated MemorySegment.
    arena.close();
  }

}
