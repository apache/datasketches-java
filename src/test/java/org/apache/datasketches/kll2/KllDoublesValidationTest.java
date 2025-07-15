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

package org.apache.datasketches.kll2;

import static org.apache.datasketches.common.Util.isOdd;

import org.testng.Assert;
import org.testng.annotations.Test;

/* A test record contains:
   0. testIndex
   1. K
   2. N
   3. stride (for generating the input)
   4. numLevels
   5. numSamples
   6. hash of the retained samples
*/

// These results are for the version that delays the roll up until the next value comes in.
// The @Test annotations have to be enabled to use this class and a section in KllDoublesHelper also
// needs to be enabled.
@SuppressWarnings("unused")
public class KllDoublesValidationTest {

  //Used only with manual running of checkTestResults(..)
  private static final long[] correctResultsWithReset = {
      0, 200, 180, 3246533, 1, 180, 1098352976109474698L,
      1, 200, 198, 8349603, 1, 198, 686681527497651888L,
      2, 200, 217, 676491, 2, 117, 495856134049157644L,
      3, 200, 238, 3204507, 2, 138, 44453438498725402L,
      4, 200, 261, 2459373, 2, 161, 719830627391926938L,
      5, 200, 287, 5902143, 2, 187, 389303173170515580L,
      6, 200, 315, 5188793, 2, 215, 985218890825795000L,
      7, 200, 346, 801923, 2, 246, 589362992166904413L,
      8, 200, 380, 2466269, 2, 280, 1081848693781775853L,
      9, 200, 418, 5968041, 2, 318, 533825689515788397L,
      10, 200, 459, 3230027, 2, 243, 937332670315558786L,
      11, 200, 504, 5125875, 2, 288, 1019197831515566845L,
      12, 200, 554, 4195571, 3, 230, 797351479150148224L,
      13, 200, 609, 2221181, 3, 285, 451246040374318529L,
      14, 200, 669, 5865503, 3, 345, 253851269470815909L,
      15, 200, 735, 831703, 3, 411, 491974970526372303L,
      16, 200, 808, 4830785, 3, 327, 1032107507126916277L,
      17, 200, 888, 1356257, 3, 407, 215225420986342944L,
      18, 200, 976, 952071, 3, 417, 600280049738270697L,
      19, 200, 1073, 6729833, 3, 397, 341758522977365969L,
      20, 200, 1180, 6017925, 3, 406, 1080227312339182949L,
      21, 200, 1298, 4229891, 3, 401, 1092460534756675086L,
      22, 200, 1427, 7264889, 4, 320, 884533400696890024L,
      23, 200, 1569, 5836327, 4, 462, 660575800011134382L,
      24, 200, 1725, 5950087, 4, 416, 669373957401387528L,
      25, 200, 1897, 2692555, 4, 406, 607308667566496888L,
      26, 200, 2086, 1512443, 4, 459, 744260340112029032L,
      27, 200, 2294, 2681171, 4, 434, 199120609113802485L,
      28, 200, 2523, 3726521, 4, 450, 570993497599288304L,
      29, 200, 2775, 2695247, 4, 442, 306717093329516310L,
      30, 200, 3052, 5751175, 5, 400, 256024589545754217L,
      31, 200, 3357, 1148897, 5, 514, 507276662329207479L,
      32, 200, 3692, 484127, 5, 457, 1082660223488175122L,
      33, 200, 4061, 6414559, 5, 451, 620820308918522117L,
      34, 200, 4467, 5587461, 5, 466, 121975084804459305L,
      35, 200, 4913, 1615017, 5, 483, 152986529342916376L,
      36, 200, 5404, 6508535, 5, 492, 858526451332425960L,
      37, 200, 5944, 2991657, 5, 492, 624906434274621995L,
      38, 200, 6538, 6736565, 6, 511, 589153542019036049L,
      39, 200, 7191, 1579893, 6, 507, 10255312374117907L,
      40, 200, 7910, 412509, 6, 538, 570863587164194186L,
      41, 200, 8701, 1112089, 6, 477, 553100668286355347L,
      42, 200, 9571, 1258813, 6, 526, 344845406406036297L,
      43, 200, 10528, 1980049, 6, 508, 411846569527905064L,
      44, 200, 11580, 2167127, 6, 520, 966876726203675488L,
      45, 200, 12738, 1975435, 7, 561, 724125506920592732L,
      46, 200, 14011, 4289627, 7, 560, 753686005174215572L,
      47, 200, 15412, 5384001, 7, 494, 551637841878573955L,
      48, 200, 16953, 2902685, 7, 560, 94602851752354802L,
      49, 200, 18648, 4806445, 7, 562, 597672400688514221L,
      50, 200, 20512, 2085, 7, 529, 417280161591969960L,
      51, 200, 22563, 6375939, 7, 558, 11300453985206678L,
      52, 200, 24819, 7837057, 7, 559, 283668599967437754L,
      53, 200, 27300, 6607975, 8, 561, 122183647493325363L,
      54, 200, 30030, 1519191, 8, 550, 1145227891427321202L,
      55, 200, 33033, 808061, 8, 568, 71070843834364939L,
      56, 200, 36336, 2653529, 8, 570, 450311772805359006L,
      57, 200, 39969, 2188957, 8, 561, 269670427054904115L,
      58, 200, 43965, 5885655, 8, 539, 1039064186324091890L,
      59, 200, 48361, 6185889, 8, 574, 178055275082387938L,
      60, 200, 53197, 208767, 9, 579, 139766040442973048L,
      61, 200, 58516, 2551345, 9, 569, 322655279254252950L,
      62, 200, 64367, 1950873, 9, 569, 101542216315768285L,
      63, 200, 70803, 2950429, 9, 582, 72294008568551853L,
      64, 200, 77883, 3993977, 9, 572, 299014330559512530L,
      65, 200, 85671, 428871, 9, 585, 491351721800568188L,
      66, 200, 94238, 6740849, 9, 577, 656204268858348899L,
      67, 200, 103661, 2315497, 9, 562, 829926273188300764L,
      68, 200, 114027, 5212835, 10, 581, 542222554617639557L,
      69, 200, 125429, 4213475, 10, 593, 713339189579860773L,
      70, 200, 137971, 2411583, 10, 592, 649651658985845357L,
      71, 200, 151768, 5243307, 10, 567, 1017459402785275179L,
      72, 200, 166944, 2468367, 10, 593, 115034451827634398L,
      73, 200, 183638, 2210923, 10, 583, 365735165000548572L,
      74, 200, 202001, 321257, 10, 591, 928479940794929153L,
      75, 200, 222201, 8185105, 11, 600, 780163958693677795L,
      76, 200, 244421, 6205349, 11, 598, 132454307780236135L,
      77, 200, 268863, 3165901, 11, 600, 369824066179493948L,
      78, 200, 295749, 2831723, 11, 595, 80968411797441666L,
      79, 200, 325323, 464193, 11, 594, 125773061716381917L,
      80, 200, 357855, 7499035, 11, 576, 994150328579932916L,
      81, 200, 393640, 1514479, 11, 596, 111092193875842594L,
      82, 200, 433004, 668493, 12, 607, 497338041653302784L,
      83, 200, 476304, 3174931, 12, 606, 845986926165673887L,
      84, 200, 523934, 914611, 12, 605, 354993119685278556L,
      85, 200, 576327, 7270385, 12, 602, 937679531753465428L,
      86, 200, 633959, 1956979, 12, 598, 659413123921208266L,
      87, 200, 697354, 3137635, 12, 606, 874228711599628459L,
      88, 200, 767089, 214923, 12, 608, 1077644643342432307L,
      89, 200, 843797, 3084545, 13, 612, 79317113064339979L,
      90, 200, 928176, 7800899, 13, 612, 357414065779796772L,
      91, 200, 1020993, 6717253, 13, 615, 532723577905833296L,
      92, 200, 1123092, 5543015, 13, 614, 508695073250223746L,
      93, 200, 1235401, 298785, 13, 616, 34344606952783179L,
      94, 200, 1358941, 4530313, 13, 607, 169924026179364121L,
      95, 200, 1494835, 4406457, 13, 612, 1026773494313671061L,
      96, 200, 1644318, 1540983, 13, 614, 423454640036650614L,
      97, 200, 1808749, 7999631, 14, 624, 466122870338520329L,
      98, 200, 1989623, 4295537, 14, 621, 609309853701283445L,
      99, 200, 2188585, 7379971, 14, 622, 141739898871015642L,
      100, 200, 2407443, 6188931, 14, 621, 22515080776738923L,
      101, 200, 2648187, 6701239, 14, 619, 257441864177795548L,
      102, 200, 2913005, 2238709, 14, 623, 867028825821064773L,
      103, 200, 3204305, 5371075, 14, 625, 1110615471273395112L,
      104, 200, 3524735, 7017341, 15, 631, 619518037415974467L,
      105, 200, 3877208, 323337, 15, 633, 513230912593541122L,
      106, 200, 4264928, 6172471, 15, 628, 885861662583325072L,
      107, 200, 4691420, 5653803, 15, 633, 754052473303005204L,
      108, 200, 5160562, 1385265, 15, 630, 294993765757975100L,
      109, 200, 5676618, 4350899, 15, 617, 1073144684944932303L,
      110, 200, 6244279, 1272235, 15, 630, 308982934296855020L,
      111, 200, 6868706, 1763939, 16, 638, 356231694823272867L,
      112, 200, 7555576, 3703411, 16, 636, 20043268926300101L,
      113, 200, 8311133, 6554171, 16, 637, 121111429906734123L,
  };

  private static int[] makeInputArray(int n, int stride) {
    assert isOdd(stride);
    int mask = (1 << 23) - 1; // because library items are single-precision floats
    int cur = 0;
    int[] arr = new int[n];
    for (int i = 0; i < n; i++) {
      cur += stride;
      cur &= mask;
      arr[i] = cur;
    }
    return arr;
  }

  //@Test //only enabled to test the above makeInputArray(..)
  public void testMakeInputArray() {
    final int[] array = { 3654721, 7309442, 2575555, 6230276, 1496389, 5151110 };
    Assert.assertEquals(makeInputArray(6, 3654721), array);
  }

  private static long simpleHashOfSubArray(final double[] arr, final int start, final int subLength) {
    final long multiplier = 738219921; // an arbitrary odd 30-bit number
    final long mask60 = (1L << 60) - 1;
    long accum = 0;
    for (int i = start; i < (start + subLength); i++) {
      accum += (long) arr[i];
      accum *= multiplier;
      accum &= mask60;
      accum ^= accum >> 30;
    }
    return accum;
  }

  //@Test //only enabled to test the above simpleHashOfSubArray(..)
  public void testHash() {
    double[] array = { 907500, 944104, 807020, 219921, 678370, 955217, 426885 };
    Assert.assertEquals(simpleHashOfSubArray(array, 1, 5), 1141543353991880193L);
  }

  /*
   * Please note that this test should be run with a modified version of KllDoublesHelper
   *  that chooses directions alternately instead of randomly.
   *  See the instructions at the bottom of that class.
   */

  //@Test  //NEED TO ENABLE HERE AND BELOW FOR VALIDATION
  public void checkTestResults() {
    int numTests = correctResultsWithReset.length / 7;
    for (int testI = 0; testI < numTests; testI++) {
      //KllDoublesHelper.nextOffset = 0;                     //NEED TO ENABLE
      assert (int) correctResultsWithReset[7 * testI] == testI;
      int k = (int) correctResultsWithReset[(7 * testI) + 1];
      int n = (int) correctResultsWithReset[(7 * testI) + 2];
      int stride = (int) correctResultsWithReset[(7 * testI) + 3];
      int[] inputArray = makeInputArray(n, stride);
      KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance(k);
      for (int i = 0; i < n; i++) {
        sketch.update(inputArray[i]);
      }
      int numLevels = sketch.getNumLevels();
      int numSamples = sketch.getNumRetained();
      int[] levels = sketch.getLevelsArray(sketch.sketchStructure);
      long hashedSamples = simpleHashOfSubArray(sketch.getDoubleItemsArray(), levels[0], numSamples);
      System.out.print(testI);
      assert correctResultsWithReset[(7 * testI) + 4] == numLevels;
      assert correctResultsWithReset[(7 * testI) + 5] == numSamples;
      assert correctResultsWithReset[7 * testI + 6] == hashedSamples;
      if (correctResultsWithReset[(7 * testI) + 6] == hashedSamples) {
        System.out.println(" pass");
      } else {
        System.out.print(" " + correctResultsWithReset[(7 * testI) + 6] + " != " + hashedSamples);
        System.out.println(" fail");
        System.out.println(sketch.toString(true, true));
        break;
      }
    }
  }

}
