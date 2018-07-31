package com.yahoo.sketches.theta;

import com.yahoo.sketches.Family;

/**
 * @author eshcar
 */
public class ConcurrentThetaFactory {

  public static ConcurrentUpdateSketch createConcurrentUpdateSketch(final Family family) {
    final UpdateSketchBuilder usb = new UpdateSketchBuilder();
    usb.setFamily(family);
    final UpdateSketch sketch = usb.build();
    return new ConcurrentUpdateSketch(sketch);
  }

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
