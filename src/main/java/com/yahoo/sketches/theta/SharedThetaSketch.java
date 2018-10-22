package com.yahoo.sketches.theta;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An interface to define the API of a shared theta sketch.
 * @author eshcar
 */
public interface SharedThetaSketch {
  long NOT_SINGLE_HASH = -1L;

  double getEstimationSnapshot();

  void updateEstimationSnapshot();

  long getVolatileTheta();

  void updateVolatileTheta();

  boolean isPropagationInProgress();

  /**
   * Propagates the given sketch or hash value into this sketch
   * @param localPropagationInProgress the flag to be updated when done
   * @param sketchIn any Theta sketch with the data
   * @param singleHash a single hash value
   */
  void propagate(final AtomicBoolean localPropagationInProgress,
      final Sketch sketchIn,
      final long singleHash);

  void startPropagation();

  void endPropagation(AtomicBoolean localPropagationInProgress);

  boolean validateEpoch(long epoch);

  void updateSingle(long hash);

  // For characterization tests
  void resetShared();

  void rebuildShared();

  CompactSketch compactShared();

}
