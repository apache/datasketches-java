package com.yahoo.sketches.theta;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author eshcar
 */
public class ConcurrentThetaContext extends UpdateSketchComposition {

  UpdateSketch localSketch_;
  private AtomicBoolean propagationInProgress_;

  ConcurrentThetaContext(final ConcurrentUpdateSketch shared, final UpdateSketch local) {
    super(shared);
    localSketch_ = local;
    propagationInProgress_ = new AtomicBoolean(false);
    localSketch_.setThetaLong(shared.getVolatileTheta());
  }

  ConcurrentThetaContext(final ConcurrentUpdateSketch shared, final UpdateSketchBuilder usb) {
    super(shared);
    localSketch_ = usb.build();
    propagationInProgress_ = new AtomicBoolean(false);
    localSketch_.setThetaLong(shared.getVolatileTheta());
  }

  public ConcurrentUpdateSketch getSharedSketch() {
    return (ConcurrentUpdateSketch) delegatee_;
  }


  @Override
  UpdateReturnState hashUpdate(final long hash) {
    final UpdateReturnState ret = localSketch_.hashUpdate(hash);
    if (localSketch_.isOutOfSpace(localSketch_.getRetainedEntries(false) + 1)
        && (localSketch_.getLgArrLongs() > localSketch_.getLgNomLongs())) {
        // local theta is going to be changed upon next update
        propagateToSharedSketch();
    }
    return ret;
  }

  @Override
  public double getEstimate() {
    return getSharedSketch().getEstimationSnapshot();
  }

  private void propagateToSharedSketch() {
    while (propagationInProgress_.get()) {} //busy wait

    final CompactSketch compactSketch = localSketch_.compact();
    propagationInProgress_.set(true);
    getSharedSketch().propagate(compactSketch, propagationInProgress_);
    localSketch_.reset();
    localSketch_.setThetaLong(getSharedSketch().getVolatileTheta());
  }

}
