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

import static cn.edu.thu.common.Utils.isQuoted;
import static cn.edu.thu.common.Utils.removeQuote;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.IndexedSchema;
import cn.edu.thu.common.IndexedSchema.MapIndexedSchema;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.RecordBatch;
import cn.edu.thu.common.Schema;
import cn.edu.thu.common.Utils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVReader extends BasicReader {

  private static final Logger logger = LoggerFactory.getLogger(CSVReader.class);

  private final int defaultPrecision = 8;
  private IndexedSchema overallSchema;
  private Schema currentFileSchema;
  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
  private List<Function<String, Long>> timeParsers = Arrays.asList(this::parseTimeAsLong,
      this::parseTimeWithFormat);
  private int preferredTimeParserIndex = 0;

  public CSVReader(Config config, List<String> files) {
    super(config, files);
    if (!config.splitFileByDevice) {
      logger.info("Collecting the overall schema");
      overallSchema = collectSchemaFromFiles(files);
      logger.info("The overall schema is collected, {}", overallSchema.brief());
      logger.debug("The overall schema is: {}", overallSchema);
    }
  }

  private IndexedSchema collectSchemaFromFiles(List<String> files) {
    SchemaSet schemaSet = new SchemaSet();
    logger.info("Collecting schema from {} files", files.size());
    for (int i = 0; i < files.size(); i++) {
      String file = files.get(i);
      logger.info("Collecting schema from {} ({}/{})", file, (i+1), files.size());
      schemaSet.union(collectSchemaFromFile(file));
    }
    return schemaSet.toSchema();
  }

  private Schema collectSchemaFromFile(String file) {
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String headerLine = reader.readLine();
      if (headerLine != null) {
        return convertHeaderToSchema(headerLine, reader, file, false);
      }
    } catch (IOException e) {
      logger.warn("Cannot read schema from file {}, file skipped", file);
    }

    return null;
  }

  private void inferTypeWithData(int fieldNum, Schema schema, BufferedReader reader,
      boolean fillCache) throws IOException {
    Set<Integer> unknownTypeIndices = new HashSet<>();
    for (int i = 0; i < fieldNum; i++) {
      unknownTypeIndices.add(i);
    }

    String line;
    List<Integer> indexToRemove = new ArrayList<>();
    int numRead = 0;
    while ((line = reader.readLine()) != null
        && !unknownTypeIndices.isEmpty()
        && ++numRead <= config.INFER_TYPE_MAX_RECORD_NUM) {
      String[] lineSplit = line.split(config.CSV_SEPARATOR);
      indexToRemove.clear();

      for (Integer unknownTypeIndex : unknownTypeIndices) {
        if (unknownTypeIndex + 1 >= lineSplit.length) {
          continue;
        }

        String field = lineSplit[unknownTypeIndex + 1];
        Class<?> aClass = inferType(field);
        if (aClass != null) {
          schema.getTypes()[unknownTypeIndex] = aClass;
          indexToRemove.add(unknownTypeIndex);
        }
      }

      unknownTypeIndices.removeAll(indexToRemove);
      if (fillCache) {
        cachedLines.add(line);
      }
    }
  }

  private Schema convertHeaderToSchema(String headerLine, BufferedReader reader, String fileName,
      boolean fillCache)
      throws IOException {
    String[] split = headerLine.split(config.CSV_SEPARATOR);
    Schema schema = new Schema();

    // the first field is fixed to time
    int fieldNum = split.length - 1;
    schema.setFields(new String[fieldNum]);
    schema.setPrecision(new int[fieldNum]);

    int devicePos = split[1].lastIndexOf('.');
    String tag;
    if (devicePos == -1) {
      tag = fileNameToTag(new File(fileName).getName());
    } else {
      tag = split[1].substring(0, devicePos);
    }

    schema.setTag(tag);
    for (int i = 1; i < split.length; i++) {
      String columnName = split[i];
      String measurement = devicePos != -1 ? columnName.substring(devicePos + 1) : columnName;
      schema.getFields()[i - 1] = measurement;
      schema.getPrecision()[i - 1] = defaultPrecision;
    }

    // infer datatype using at most a batch of lines
    if (overallSchema == null) {
      inferTypeWithData(fieldNum, schema, reader, fillCache);
    } else {
      inferTypeWithOverallSchema(schema);
    }

    return schema;
  }

  private String fileNameToTag(String fileName) {
    int suffixPos = fileName.lastIndexOf('.');
    return fileName.substring(0, suffixPos);
  }

  private void inferTypeWithOverallSchema(Schema schema) {
    for (int i = 0; i < schema.getFields().length; i++) {
      String field = schema.getFields()[i];
      schema.getTypes()[i] = overallSchema.getTypes()[overallSchema.getIndex(field)];
    }
  }

  private Class<?> inferType(String field) {
    if (field.equalsIgnoreCase("null") || field.isEmpty()) {
      return null;
    }

    if (isQuoted(field)) {
      field = removeQuote(field);
      if (field.equalsIgnoreCase("null") || field.isEmpty()) {
        return null;
      }
      return String.class;
    }

    try {
      Long.parseLong(field);
      return Long.class;
    } catch (NumberFormatException ignore) {
      // ignored
    }

    try {
      Double.parseDouble(field);
      return Double.class;
    } catch (NumberFormatException ignore) {
      // ignored
    }

    return String.class;
  }

  @Override
  public RecordBatch convertCachedLinesToRecords() {
    RecordBatch records = new RecordBatch();
    for (String cachedLine : cachedLines) {
      try {
        records.add(convertToRecord(cachedLine));
      } catch (Exception e) {
        logger.warn("Skipping mal-formatted record {}, ", cachedLine, e);
      }
    }
    stat.onBatchGenerated(records, getCurrentSchema());
    return records;
  }

  private Object parseField(String field, Schema schema, int index) {
    if (field.isEmpty() || field.equalsIgnoreCase("null")) {
      return null;
    }

    Class<?> type = schema.getTypes()[index];
    if (type == Long.class) {
      return Long.parseLong(field);
    }
    if (type == Double.class) {
      return Double.parseDouble(field);
    }
    return field;
  }

  private List<Object> fieldsWithCurrentFileSchema(String[] split) {
    List<Object> fields = new ArrayList<>(currentFileSchema.getFields().length);
    for (int i = 1; i < split.length; i++) {
      split[i] = Utils.removeQuote(split[i]);

      fields.add(parseField(split[i], currentFileSchema, i - 1));
    }
    return fields;
  }

  private List<Object> fieldsWithOverallSchema(String[] split) {
    List<Object> fields = new ArrayList<>(overallSchema.getFields().length);
    for (int i = 0; i < overallSchema.getFields().length; i++) {
      fields.add(null);
    }

    for (int i = 1; i < split.length && i - 1 < currentFileSchema.getFields().length; i++) {
      int overallIndex = overallSchema.getIndex(currentFileSchema.getFields()[i - 1]);
      split[i] = Utils.removeQuote(split[i]);

      fields.set(overallIndex, parseField(split[i], overallSchema, overallIndex));
    }
    return fields;
  }

  private long parseTime(String timeStr) throws ParseException {
    for (int i = 0; i < timeParsers.size(); i++) {
      int index = (preferredTimeParserIndex + i) % timeParsers.size();
      Long time = timeParsers.get(index).apply(timeStr);
      if (time != null) {
        preferredTimeParserIndex = index;
        return time;
      }
    }

    throw new ParseException("Cannot parse time string " + timeStr, 0);
  }

  private Long parseTimeWithFormat(String timeStr) {
    try {
      return dateFormat.parse(timeStr).getTime();
    } catch (ParseException e) {
      // ignore
    }
    return null;
  }

  private Long parseTimeAsLong(String timeStr) {
    try {
      return Long.parseLong(timeStr);
    } catch (NumberFormatException e) {
      // ignore
    }
    return null;
  }

  private Record convertToRecord(String line) throws ParseException {
    Record record;
    String[] split = line.split(config.CSV_SEPARATOR);
    long time;
    time = parseTime(split[0]);
    String tag = currentFileSchema.getTag();
    List<Object> fields;

    if (config.splitFileByDevice) {
      fields = fieldsWithCurrentFileSchema(split);
    } else {
      fields = fieldsWithOverallSchema(split);
    }
    record = new Record(time, tag, fields);

    return record;
  }

  @Override
  public void onFileOpened() {
    Schema fileSchema;
    try {
      fileSchema = convertHeaderToSchema(reader.readLine(), reader, currentFile, true);
      stat.onSchemaGenerated(fileSchema);
      logger.info("File {} schema collected", currentFile);
      logger.debug("Current file schema: {}", fileSchema);
    } catch (IOException e) {
      logger.warn("Cannot read schema from {}, skipping", currentFile);
      return;
    }
    currentFileSchema = fileSchema;
  }

  @Override
  public Schema getCurrentSchema() {
    return config.splitFileByDevice ? currentFileSchema : overallSchema;
  }

  private static class SchemaSet {

    private final Map<String, Integer> fieldPrecisionMap = new HashMap<>();
    private final Map<String, Class<?>> fieldTypeMap = new HashMap<>();

    public void union(Schema schema) {
      if (schema == null) {
        return;
      }

      for (int i = 0; i < schema.getFields().length; i++) {
        int finalI = i;
        fieldPrecisionMap.compute(schema.getFields()[i], (s, p) -> Math.max(p == null ? 0 : p,
            schema.getPrecision()[finalI]));
        fieldTypeMap.compute(schema.getFields()[i], (s, t) -> {
          Class<?> newType = schema.getTypes()[schema.getIndex(s)];
          return mergeType(t, newType);
        });
      }
    }

    private Class<?> mergeType(Class<?> t1, Class<?> t2) {
      if (t1 == null && t2 == null) {
        return null;
      }
      if (t1 == null) {
        return t2;
      }
      if (t2 == null) {
        return t1;
      }

      if (t1 == Double.class && t2 == Long.class || t1 == Long.class && t2 == Double.class) {
        return Double.class;
      }

      if (t1 != t2) {
        return String.class;
      }
      return t1;
    }

    public IndexedSchema toSchema() {
      MapIndexedSchema schema = new MapIndexedSchema();
      schema.setFields(new String[fieldPrecisionMap.size()]);
      schema.setPrecision(new int[fieldPrecisionMap.size()]);

      int index = 0;
      for (Entry<String, Integer> entry : fieldPrecisionMap.entrySet()) {
        schema.getFields()[index] = entry.getKey();
        schema.getTypes()[index] = fieldTypeMap.getOrDefault(entry.getKey(), String.class);
        schema.getPrecision()[index++] = entry.getValue();
      }

      return schema.rebuildIndex();
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    stat.report();
  }
}
