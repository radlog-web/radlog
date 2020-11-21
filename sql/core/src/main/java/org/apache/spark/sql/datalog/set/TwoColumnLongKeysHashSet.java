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

import org.apache.spark.sql.catalyst.InternalRow;

public class TwoColumnLongKeysHashSet extends it.unimi.dsi.fastutil.longs.LongOpenHashSet implements HashSet {

    public void insert(InternalRow row) {
        this.add(getKeyL(row));
    }

    public void ifNotExistsInsert(InternalRow row, HashSet diffSet) {
        // save time on converting between formats by doing it once
        long key = getKeyL(row);
        if (!this.contains(key))
            ((TwoColumnLongKeysHashSet)diffSet).add(key);
    }

    public HashSet union(HashSet other) {
        super.addAll((TwoColumnLongKeysHashSet)other);
        return this;
    }

    protected long getKeyL(InternalRow row) {
        return ((long) row.getInt(0) << 32) | (row.getInt(1) & 0xffffffffL);
    }

    public void clear() {
        super.clear();
    }
}
