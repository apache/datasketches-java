/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.req;

import java.util.List;

/**
 * The implementation of the ReqDebug interface. The current levels are
 * implemented:
 * <ul><li><b>Level 0: </b>The random generator in the compactor will be given a fixed
 * seed which will make the sketch deterministic.</li>
 * <li><b>Level 1: </b>Print summaries, but not the data retained by the sketch. This is useful
 * when the sketch is large.</li>
 * <li><b>Level 2: </b>Print summaries and all data retained by the sketch.</li>
 * </ul>
 *
 * @author Lee Rhodes
 */
public class ReqDebugImpl implements ReqDebug {
  private static final String LS = System.getProperty("line.separator");
  private static final String TAB = "\t";
  private ReqSketch sk;
  final int debugLevel;
  final String fmt;

  /**
   * Constructor
   * @param debugLevel sets the debug level of detail
   * @param fmt string format to use when printing values
   */
  public ReqDebugImpl(final int debugLevel, final String fmt) {
    this.debugLevel = debugLevel;
    this.fmt = fmt;
  }

  @Override
  public void emitStart(final ReqSketch sk) {
    if (debugLevel == 0) { return; }
    this.sk = sk;
    println("START");
  }

  @Override
  public void emitStartCompress() {
    if (debugLevel == 0) { return; }
    final int retItems = sk.getRetainedItems();
    final int maxNomSize = sk.getMaxNomSize();
    final long totalN = sk.getN();
    final StringBuilder sb = new StringBuilder();
    sb.append("COMPRESS: ");
    sb.append("skRetItems: ").append(retItems).append(" >= ");
    sb.append("MaxNomSize: ").append(maxNomSize);
    sb.append("  N: ").append(totalN);
    println(sb.toString());
    emitAllHorizList();
  }

  @Override
  public void emitCompressDone() {
    if (debugLevel == 0) { return; }
    final int retItems = sk.getRetainedItems();
    final int maxNomSize = sk.getMaxNomSize();
    emitAllHorizList();
    println("COMPRESS: DONE: SketchSize: " + retItems + TAB
        + " MaxNomSize: " + maxNomSize + LS + LS);
  }

  @Override
  public void emitAllHorizList() {
    if (debugLevel == 0) { return; }
    final List<ReqCompactor> compactors = sk.getCompactors();
    for (int h = 0; h < sk.getCompactors().size(); h++) {
      final ReqCompactor c = compactors.get(h);
      println(c.toListPrefix());
      if (debugLevel > 1) {
        print(c.getBuffer().toHorizList(fmt, 20) + LS);
      } else {
        print(LS);
      }
    }
  }

  @Override
  public void emitMustAddCompactor() {
    if (debugLevel == 0) { return; }
    final int curLevels = sk.getNumLevels();
    final List<ReqCompactor> compactors = sk.getCompactors();
    final ReqCompactor topC = compactors.get(curLevels - 1);
    final int lgWt = topC.getLgWeight();
    final int retCompItems = topC.getBuffer().getCount();
    final int nomCap = topC.getNomCapacity();
    final StringBuilder sb = new StringBuilder();
    sb.append("  ");
    sb.append("Must Add Compactor: len(c[").append(lgWt).append("]): ");
    sb.append(retCompItems).append(" >= c[").append(lgWt).append("].nomCapacity(): ")
      .append(nomCap);
    println(sb.toString());
  }

  //compactor signals

  @Override
  public void emitCompactingStart(final byte lgWeight) {
    if (debugLevel == 0) { return; }
    final List<ReqCompactor> compactors = sk.getCompactors();
    final ReqCompactor comp = compactors.get(lgWeight);
    final int nomCap = comp.getNomCapacity();
    final int secSize = comp.getSectionSize();
    final int numSec = comp.getNumSections();
    final long state = comp.getState();
    final int bufCap = comp.getBuffer().getCapacity();
    final StringBuilder sb = new StringBuilder();
    sb.append(LS + "  ");
    sb.append("COMPACTING[").append(lgWeight).append("] ");
    sb.append("NomCapacity: ").append(nomCap);
    sb.append(TAB + " SectionSize: ").append(secSize);
    sb.append(TAB + " NumSections: ").append(numSec);
    sb.append(TAB + " State(bin): ").append(Long.toBinaryString(state));
    sb.append(TAB + " BufCapacity: ").append(bufCap);
    println(sb.toString());
  }

  @Override
  public void emitNewCompactor(final byte lgWeight) {
    if (debugLevel == 0) { return; }
    final List<ReqCompactor> compactors = sk.getCompactors();
    final ReqCompactor comp = compactors.get(lgWeight);
    println("    New Compactor: lgWeight: " + comp.getLgWeight()
        + TAB + "sectionSize: " + comp.getSectionSize()
        + TAB + "numSections: " + comp.getNumSections());
  }

  @Override
  public void emitAdjSecSizeNumSec(final byte lgWeight) {
    if (debugLevel == 0) { return; }
    final List<ReqCompactor> compactors = sk.getCompactors();
    final ReqCompactor comp = compactors.get(lgWeight);
    final int secSize = comp.getSectionSize();
    final int numSec = comp.getNumSections();
    final StringBuilder sb = new StringBuilder();
    sb.append("    ");
    sb.append("Adjust: SectionSize: ").append(secSize);
    sb.append(" NumSections: ").append(numSec);
    println(sb.toString());
  }

  @Override
  public void emitCompactionDetail(final int compactionStart, final int compactionEnd,
      final int secsToCompact, final int promoteLen, final boolean coin) {
    if (debugLevel == 0) { return; }
    final StringBuilder sb = new StringBuilder();
    sb.append("    ");
    sb.append("SecsToCompact: ").append(secsToCompact);
    sb.append(TAB + " CompactStart: ").append(compactionStart);
    sb.append(TAB + " CompactEnd: ").append(compactionEnd).append(LS);
    final int delete = compactionEnd - compactionStart;
    final String oddOrEven = coin ? "Odds" : "Evens";
    sb.append("    ");
    sb.append("Promote: ").append(promoteLen);
    sb.append(TAB + " Delete: ").append(delete);
    sb.append(TAB + " Choose: ").append(oddOrEven);
    println(sb.toString());
  }

  @Override
  public void emitCompactionDone(final byte lgWeight) {
    if (debugLevel == 0) { return; }
    final List<ReqCompactor> compactors = sk.getCompactors();
    final ReqCompactor comp = compactors.get(lgWeight);
    final long state = comp.getState();
    println("  COMPACTING DONE: NumCompactions: " + state + LS);
  }

  static final void printf(final String format, final Object ...args) {
    System.out.printf(format, args);
  }

  static final void print(final Object o) { System.out.print(o.toString()); }

  static final void println(final Object o) { System.out.println(o.toString()); }

}
