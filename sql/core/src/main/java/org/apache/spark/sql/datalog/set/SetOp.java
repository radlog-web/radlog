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

package org.apache.spark.sql.datalog.set;

import scala.collection.Iterator;

import org.apache.spark.sql.catalyst.InternalRow;

public class SetOp {
  public static HashSet diff(Iterator<InternalRow> iter, HashSet allSet) {
    if (allSet instanceof IntKeysHashSet) {

      IntKeysHashSet diffSet = new IntKeysHashSet();
      while (iter.hasNext()) {
        InternalRow row = iter.next();
        int key = row.getInt(0);
        if (!((IntKeysHashSet) allSet).contains(key)) {
          diffSet.add(key);
        }
      }
      return diffSet;

    } else if (allSet instanceof TwoColumnLongKeysHashSet) {

      TwoColumnLongKeysHashSet diffSet = new TwoColumnLongKeysHashSet();
      while (iter.hasNext()) {
        InternalRow row = iter.next();
        long key = ((long) row.getInt(0) << 32) | (row.getInt(1) & 0xffffffffL);
        if (!((TwoColumnLongKeysHashSet) allSet).contains(key)) {
          diffSet.add(key);
        }
      }
      return diffSet;

    } else if (allSet instanceof ObjectHashSet) {
      ObjectHashSet diffSet = new ObjectHashSet();
      while (iter.hasNext()) {
        InternalRow row = iter.next();
        InternalRow key = row.copy();
        if (!((ObjectHashSet) allSet).contains(key)) {
          diffSet.add(key);
        }
      }
      return diffSet;
    } else {
      throw new UnsupportedOperationException("Set Type not supported: " + allSet.getClass().getSimpleName());
    }
  }

  public static HashSet[] initDeltaAllRDDHashSets(Iterator<InternalRow> iter, int outputLength) {
    if (outputLength == 1) {

      IntKeysHashSet deltaSet = new IntKeysHashSet();
      IntKeysHashSet allSet = new IntKeysHashSet();
      while (iter.hasNext()) {
        InternalRow row = iter.next();
        int key = row.getInt(0);
        deltaSet.add(key);
        allSet.add(key);
      }
      return new HashSet[]{deltaSet, allSet};

    } else if (outputLength == 2) {

      TwoColumnLongKeysHashSet deltaSet = new TwoColumnLongKeysHashSet();
      TwoColumnLongKeysHashSet allSet = new TwoColumnLongKeysHashSet();
      while (iter.hasNext()) {
        InternalRow row = iter.next();
        long key = ((long) row.getInt(0) << 32) | (row.getInt(1) & 0xffffffffL);
        deltaSet.add(key);
        allSet.add(key);
      }
      return new HashSet[]{deltaSet, allSet};

    } else if(outputLength >= 3) {
      ObjectHashSet deltaSet = new ObjectHashSet();
      ObjectHashSet allSet = new ObjectHashSet();
      while (iter.hasNext()) {
        InternalRow row = iter.next();
        InternalRow key = row.copy();
        deltaSet.add(key);
        allSet.add(key);
      }
      return new HashSet[]{deltaSet, allSet};

    } else {
      throw new UnsupportedOperationException("outputLength not supported: " + outputLength);
    }
  }

}
