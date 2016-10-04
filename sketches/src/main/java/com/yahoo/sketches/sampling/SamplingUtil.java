package com.yahoo.sketches.sampling;

import java.util.Random;

/**
 * Common utility functions for the sampling family of sketches.
 *
 * @author Jon Malkin
 */
public class SamplingUtil {

  public static final Random rand = new Random();

  private SamplingUtil() {}

  /**
   * Checks if target sampling allocation is more than 50% of max sampling size. If so, returns
   * max sampling size, otherwise passes through the target size.
   *
   * @param maxSize      Maximum allowed reservoir size, as from getK()
   * @param resizeTarget Next size based on a pure ResizeFactor scaling
   * @return <code>(reservoirSize_ &lt; 2*resizeTarget ? reservoirSize_ : resizeTarget)</code>
   */
  static int getAdjustedSize(final int maxSize, final int resizeTarget) {
    if (maxSize - (resizeTarget << 1) < 0L) {
      return maxSize;
    }
    return resizeTarget;
  }

  static int startingSubMultiple(final int lgTarget, final int lgRf, final int lgMin) {
    return (lgTarget <= lgMin)
            ? lgMin : (lgRf == 0) ? lgTarget
            : (lgTarget - lgMin) % lgRf + lgMin;
  }
}
