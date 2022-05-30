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

package cn.edu.thu.reader;

import static cn.edu.thu.common.Utils.fieldSize;

import cn.edu.thu.common.RecordBatch;
import cn.edu.thu.common.Schema;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderStatistic {

  private static final Logger logger = LoggerFactory.getLogger(ReaderStatistic.class);

  private Map<Class<?>, AtomicLong> typeNonNullPointNumMap = new HashMap<>();
  private Map<Class<?>, AtomicLong> typeTotalPointNumMap = new HashMap<>();
  private Map<Class<?>, AtomicLong> typeRawSizeMap = new HashMap<>();
  private Map<Class<?>, AtomicLong> typeColNumMap = new HashMap<>();

  void onBatchGenerated(RecordBatch batch, Schema schema) {
    batch.forEach(r -> {
      for (int i = 0; i < r.fields.size(); i++) {
        Class<?> type = schema.getTypes()[i];

        typeTotalPointNumMap.compute(type, updateTypeCntFunc(1));
        if (r.fields.get(i) != null) {
          typeNonNullPointNumMap.compute(type, updateTypeCntFunc(1));
          typeRawSizeMap.compute(type, updateTypeCntFunc(fieldSize(r.fields.get(i))));
        }
      }
    });
  }



  void onSchemaGenerated(Schema schema) {
    for (int i = 0; i < schema.getTypes().length; i++) {
      Class<?> type = schema.getTypes()[i];
      typeColNumMap.compute(type, updateTypeCntFunc(1));
    }
  }

  private BiFunction<Class<?>, AtomicLong, AtomicLong> updateTypeCntFunc(int delta) {
    return (t, oc) -> {
      if (oc == null) {
        oc = new AtomicLong();
      }
      oc.addAndGet(delta);
      return oc;
    };
  }

  public void report() {
    logger.info("Non-null points of each type: {}", typeNonNullPointNumMap);
    logger.info("Total points of each type: {}", typeTotalPointNumMap);
    logger.info("Raw size of each type: {}", typeRawSizeMap);
    logger.info("Column number of each type: {}", typeRawSizeMap);
  }
}
