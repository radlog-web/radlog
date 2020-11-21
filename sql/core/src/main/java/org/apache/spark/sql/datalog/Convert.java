/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.datalog;

import java.util.Arrays;
import java.util.Random;

class MyRandom extends Random {
  public int nextShort() {
    return next(16); // give me just 16 bits.
  }

  public int nextNonNegativeShort() {
    return next(15); // give me just 15 bits.
  }
}

public class Convert {
  private static long s_mask = 0xFFFFL;

  public static long intsToLong(int a, int b) {
    return (long) a << 32 | b & 0xFFFFFFFFL;
  }

  public static long shortsToLong(int[] shorts) {
    for (int s: shorts) {
      if (s > Short.MAX_VALUE) {
        throw new RuntimeException("Short value overflow: " + s);
      }
    }
    short a = (short) shorts[0];
    short b = (short) shorts[1];
    short c = (short) shorts[2];
    short d = (short) shorts[3];
    return (long) a << 48 | (long) b << 32 | (long) c << 16 | d & s_mask;
  }

  public static void longToInts(long l, int[] ints) {
    ints[0] = (int) (l >> 32);
    ints[1] = (int) l;
  }

  public static void longToShorts(long l, int[] shorts) {
    shorts[3] = (int) (l & s_mask);
    l = l >> 16;
    shorts[2] = (int) (l & s_mask);
    l = l >> 16;
    shorts[1] = (int) (l & s_mask);
    l = l >> 16;
    shorts[0] = (int) (l & s_mask);
  }

  private static void testMatch(int[] input) {
    System.out.println(Arrays.toString(input));
    int[] result = new int[4];
    long l = shortsToLong(input);
    longToShorts(l, result);
    for (int i = 0; i < 4; i++) {
      if (input[i] != result[i]) {
        throw new RuntimeException("Error: in - " + Arrays.toString(input) + " out - " + Arrays.toString(result));
      }
    }
  }

  public static void main(String[] args) {
    testMatch(new int[]{0, 1, 32767, 123});

    MyRandom rnd = new MyRandom();
    for (int i = 0; i < 100000; i++) {
      int[] in = new int[]{rnd.nextNonNegativeShort(), rnd.nextNonNegativeShort(), rnd.nextNonNegativeShort(), rnd.nextNonNegativeShort()};
      testMatch(in);
    }

    try {
      testMatch(new int[]{0, 32768, 0, 0});
    } catch (RuntimeException e) {
      System.out.println(e.getMessage());
    }
  }
}
