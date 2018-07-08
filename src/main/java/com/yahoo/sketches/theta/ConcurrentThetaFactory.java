package com.yahoo.sketches.theta;

import com.yahoo.sketches.Family;

/**
 * @author eshcar
 */
public class ConcurrentThetaFactory {

  static public ConcurrentUpdateSketch createConcurrentUpdateSketch(Family family) {
    UpdateSketchBuilder usb = new UpdateSketchBuilder();
    usb.setFamily(family);
    UpdateSketch sketch = usb.build();
    return new ConcurrentUpdateSketch(sketch);
  }

  static public ConcurrentThetaContext createConcurrentThetaContext(ConcurrentUpdateSketch shared) {
    UpdateSketchBuilder usb = new UpdateSketchBuilder();
    Family family = shared.getFamily();
    usb.setFamily(family);
    UpdateSketch local = usb.build();
//    return new ConcurrentThetaContext(shared, usb);
    return new ConcurrentThetaContext(shared, local);
  }

  public static ConcurrentUpdateSketch createConcurrentUpdateSketch(UpdateSketch sketch) {
    return new ConcurrentUpdateSketch(sketch);
  }

}
