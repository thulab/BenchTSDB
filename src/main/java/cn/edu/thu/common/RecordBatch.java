/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.thu.common;

import java.util.ArrayList;
import java.util.Collection;

public class RecordBatch extends ArrayList<Record> {

  private long nonNullFieldNum;
  private long allFieldNum;

  public RecordBatch() {
    super();
  }

  public RecordBatch(int batchSize) {
    super(batchSize);
  }

  public RecordBatch(Collection<Record> records) {
    super(records);
  }

  @Override
  public boolean add(Record record) {
    nonNullFieldNum += record.nonNullFieldNum;
    allFieldNum += record.fields.size();
    return super.add(record);
  }

  @Override
  public boolean addAll(Collection<? extends Record> c) {
    c.forEach(record -> {
      nonNullFieldNum += record.nonNullFieldNum;
      allFieldNum += record.fields.size();
    });
    return super.addAll(c);
  }

  public long getNonNullFieldNum() {
    return nonNullFieldNum;
  }

  public long getAllFieldNum() {
    return allFieldNum;
  }
}
