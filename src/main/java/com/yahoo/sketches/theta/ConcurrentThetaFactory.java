package com.yahoo.sketches.theta;

import com.yahoo.sketches.Family;

/**
 * @author eshcar
 */
public class ConcurrentThetaFactory {

  /**
   * Returns a new Concurrent, Heap-based, QuickSelect sketch
   * @return a new Concurrent, Heap, QuickSelect sketch
   */
  public static ConcurrentUpdateSketch createConcurrentUpdateSketch(final Family family) {
    final UpdateSketchBuilder usb = new UpdateSketchBuilder();
    usb.setFamily(family);
    final UpdateSketch sketch = usb.build();
    return new ConcurrentUpdateSketch(sketch);
  }

  /**
   * Returns a new Concurrent, Heap, QuickSelect sketch for a local thread context given the
   * target shared sketch.
   * @param shared the target shared sketch
   * @return a new Concurrent, Heap, QuickSelect sketch for a local thread context given the
   * target shared sketch.
   */
  public static ConcurrentThetaContext createConcurrentThetaContext(final ConcurrentUpdateSketch shared) {
    final UpdateSketchBuilder usb = new UpdateSketchBuilder();
    final Family family = shared.getFamily();
    usb.setFamily(family);
    final UpdateSketch local = usb.build();
    //    return new ConcurrentThetaContext(shared, usb);
    return new ConcurrentThetaContext(shared, local);
  }

  public static ConcurrentUpdateSketch createConcurrentUpdateSketch(final UpdateSketch sketch) {
    return new ConcurrentUpdateSketch(sketch);
  }

}
