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

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.RecordBatch;
import cn.edu.thu.common.Schema;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SyntheticReader extends BasicReader {

  private Schema schema;

  private int deviceCursor = 0;
  private int pointCursor = 0;
  private Random random = new Random(123456);
  private RecordBatch batch = new RecordBatch(config.BATCH_SIZE);
  private String[] deviceNames = new String[config.syntheticDeviceNum];

  public SyntheticReader(Config config) {
    super(config);
    for (int i = 0; i < config.syntheticDeviceNum; i++) {
      deviceNames[i] = "root.device_" + i;
    }
    for (int i = 0; i < config.BATCH_SIZE; i++) {
      List<Object> fields = new ArrayList<>(config.syntheticMeasurementNum);
      for (int j = 0; j < config.syntheticMeasurementNum; j++) {
        fields.add(random.nextDouble() < config.syntheticNullRatio ? null : random.nextDouble());
      }
      Record record = new Record(0, "", fields);
      batch.add(record);
    }

    schema = new Schema();
    schema.setFields(new String[config.syntheticMeasurementNum]);
    schema.setPrecision(new int[config.syntheticMeasurementNum]);
    for (int i = 0; i < config.syntheticMeasurementNum; i++) {
      schema.getFields()[i] = "sensor_" + i;
      schema.getPrecision()[i] = 4;
    }
  }

  @Override
  public boolean hasNext() {
    return deviceCursor < config.syntheticDeviceNum;
  }

  @Override
  public RecordBatch convertCachedLinesToRecords() {
    String device = deviceNames[deviceCursor];

    int i = 0;
    for (; i < config.BATCH_SIZE && pointCursor < config.syntheticPointNum; i++) {
      Record record = batch.get(i);
      record.tag = device;
      record.timestamp = pointCursor++;
      for (int j = 0; j < config.syntheticMeasurementNum; j++) {
        record.fields.set(j, random.nextDouble() < config.syntheticNullRatio ? null :
          random.nextDouble());
      }
    }
    if (pointCursor == config.syntheticPointNum) {
      deviceCursor ++;
      pointCursor = 0;
    }
    return new RecordBatch(batch.subList(0, i));
  }

  @Override
  public void onFileOpened() throws Exception {
     // no action
  }

  @Override
  public Schema getCurrentSchema() {
    return schema;
  }
}
