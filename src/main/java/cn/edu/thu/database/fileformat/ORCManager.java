package cn.edu.thu.database.fileformat;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.RecordBatch;
import cn.edu.thu.common.Schema;
import cn.edu.thu.database.IDataBaseManager;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.*;
import org.apache.orc.OrcFile.Version;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * time, seriesid, value
 * <p>
 * time, deviceId, s1, s2, s3...
 * <p>
 * time, series1, series2...
 */
public class ORCManager implements IDataBaseManager {

  private static Logger logger = LoggerFactory.getLogger(ORCManager.class);

  private Map<String, Writer> writerMap = new HashMap<>();
  private String lastTag;
  private Config config;
  private String filePath;

  private long totalFileSize = 0;

  private boolean closeOnTagChanged = true;

  public ORCManager(Config config) {
    this.config = config;
    this.filePath = config.FILE_PATH;
  }

  public ORCManager(Config config, int threadNum) {
    this.config = config;
    this.filePath = config.FILE_PATH + "_" + threadNum;
  }

  @Override
  public void initServer() {

  }

  @Override
  public void initClient() {

  }

  private Writer createWriter(String tag, Schema schema) {
    TypeDescription orcSchema = genWriteSchema(schema);

    String fullFilePath = tagToFilePath(tag);
    new File(fullFilePath).delete();
    Writer writer = null;
    try {
      writer = OrcFile.createWriter(new Path(fullFilePath),
          OrcFile.writerOptions(new Configuration())
              .setSchema(orcSchema)
              .stripeSize(64 * 1024 * 1024L)
              .blockSize(256 * 1024 * 1024L)
              .bufferSize(16 * 1024 * 1024)
              .compress(CompressionKind.SNAPPY)
              .version(Version.V_0_12));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return writer;
  }

  private void insertDoubleColumn(VectorizedRowBatch batch, int colIndex, int rowIndex,
      Record record) {
    DoubleColumnVector v;
    if (!config.splitFileByDevice) {
      v = (DoubleColumnVector) batch.cols[colIndex + 2];
    } else {
      v = (DoubleColumnVector) batch.cols[colIndex + 1];
    }
    if (record.fields.get(colIndex) != null) {
      v.vector[rowIndex] = (double) record.fields.get(colIndex);
      v.isNull[rowIndex] = false;
    } else {
      v.isNull[rowIndex] = true;
      v.noNulls = false;
    }
  }

  private void insertLongColumn(VectorizedRowBatch batch, int colIndex, int rowIndex,
      Record record) {
    LongColumnVector v;
    if (!config.splitFileByDevice) {
      v = (LongColumnVector) batch.cols[colIndex + 2];
    } else {
      v = (LongColumnVector) batch.cols[colIndex + 1];
    }
    if (record.fields.get(colIndex) != null) {
      v.vector[rowIndex] = (long) record.fields.get(colIndex);
      v.isNull[rowIndex] = false;
    } else {
      v.isNull[rowIndex] = true;
      v.noNulls = false;
    }
  }

  private void insertStringColumn(VectorizedRowBatch batch, int colIndex, int rowIndex,
      Record record) {
    BytesColumnVector v;
    if (!config.splitFileByDevice) {
      v = (BytesColumnVector) batch.cols[colIndex + 2];
    } else {
      v = (BytesColumnVector) batch.cols[colIndex + 1];
    }
    if (record.fields.get(colIndex) != null) {
      v.vector[rowIndex] = ((String) record.fields.get(colIndex)).getBytes();
      v.isNull[rowIndex] = false;
    } else {
      v.isNull[rowIndex] = true;
      v.noNulls = false;
    }
  }

  private void insertColumn(VectorizedRowBatch batch, int colIndex, int rowIndex,
      Record record, Class<?> type) {
    if (type == Long.class) {
      insertLongColumn(batch, colIndex, rowIndex, record);
    } else if (type == Double.class) {
      insertDoubleColumn(batch, colIndex, rowIndex, record);
    } else {
      insertStringColumn(batch, colIndex, rowIndex, record);
    }
  }

  @Override
  public long insertBatch(RecordBatch records, Schema schema) {

    long start = System.nanoTime();

    String tag = records.get(0).tag;
    if (closeOnTagChanged && config.splitFileByDevice && !Objects.equals(tag, lastTag)) {
      close();
    }

    Writer writer = getWriter(tag, schema);

    if (config.useAlignedSeries) {
      insertBatchAligned(records, schema, writer);
    } else {
      insertBatchNonAligned(records, schema, writer);
    }

    lastTag = tag;
    return System.nanoTime() - start;
  }

  private void insertBatchAligned(RecordBatch records, Schema schema,
      Writer writer) {
    VectorizedRowBatch batch = writer.getSchema().createRowBatch(records.size());

    for (int i = 0; i < records.size(); i++) {
      Record record = records.get(i);
      LongColumnVector time = (LongColumnVector) batch.cols[0];
      time.vector[i] = record.timestamp;

      if (!config.splitFileByDevice) {
        BytesColumnVector device = (BytesColumnVector) batch.cols[1];
        device.setVal(i, record.tag.getBytes(StandardCharsets.UTF_8));
      }

      for (int j = 0; j < schema.getFields().length; j++) {
        if (config.ignoreStrings && schema.getTypes()[j] == String.class) {
          continue;
        }
        insertColumn(batch, j, i, record, schema.getTypes()[j]);
      }

      batch.size++;

      // If the batch is full, write it out and start over. actually not needed here
      if (batch.size == batch.getMaxSize()) {
        try {
          writer.addRowBatch(batch);
        } catch (IOException e) {
          e.printStackTrace();
        }
        batch.reset();
      }
    }

    if (batch.size > 0) {
      try {
        writer.addRowBatch(batch);
      } catch (IOException e) {
        e.printStackTrace();
      }
      batch.reset();
    }
  }

  private void insertBatchNonAligned(RecordBatch records, Schema schema,
      Writer writer) {
    VectorizedRowBatch batch = writer.getSchema().createRowBatch((int) records.getNonNullFieldNum());

    for (int i = 0; i < records.size(); i++) {
      Record record = records.get(i);
      LongColumnVector time = (LongColumnVector) batch.cols[0];

      List<Object> fields = record.fields;
      for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
        if (config.ignoreStrings && schema.getTypes()[fieldIndex] == String.class) {
          continue;
        }

        Object field = fields.get(fieldIndex);
        if (field != null) {
          time.vector[batch.size] = record.timestamp;
          if (!config.splitFileByDevice) {
            BytesColumnVector device = (BytesColumnVector) batch.cols[1];
            device.setVal(batch.size, record.tag.getBytes(StandardCharsets.UTF_8));
            BytesColumnVector measurement = (BytesColumnVector) batch.cols[2];
            measurement.setVal(batch.size, schema.getFields()[fieldIndex].getBytes());
            BytesColumnVector value = (BytesColumnVector) batch.cols[3];
            value.setVal(batch.size, field.toString().getBytes());
          } else {
            BytesColumnVector measurement = (BytesColumnVector) batch.cols[1];
            measurement.setVal(batch.size, schema.getFields()[fieldIndex].getBytes());
            BytesColumnVector value = (BytesColumnVector) batch.cols[2];
            value.setVal(batch.size, field.toString().getBytes());
          }
          batch.size++;

          // If the batch is full, write it out and start over. actually not needed here
          if (batch.size == batch.getMaxSize()) {
            try {
              writer.addRowBatch(batch);
            } catch (IOException e) {
              e.printStackTrace();
            }
            batch.reset();
          }
        }
      }
    }

    if (batch.size > 0) {
      try {
        writer.addRowBatch(batch);
      } catch (IOException e) {
        e.printStackTrace();
      }
      batch.reset();
    }
  }

  private Writer getWriter(String tag, Schema schema) {
    if (!config.splitFileByDevice) {
      return writerMap.computeIfAbsent(Config.DEFAULT_TAG, t -> createWriter(t, schema));
    } else {
      return writerMap.computeIfAbsent(tag, t -> createWriter(t, schema));
    }
  }

  private String tagToFilePath(String tag) {
    if (config.splitFileByDevice) {
      return filePath + "_" + tag;
    } else {
      return filePath + "_" + Config.DEFAULT_TAG;
    }
  }

  private TypeDescription genWriteSchema(Schema schema) {
    if (config.useAlignedSeries) {
      return genAlignedWriteSchema(schema);
    } else {
      return genNonAlignedWriteSchema(schema);
    }
  }

  private TypeDescription genAlignedWriteSchema(Schema schema) {
    TypeDescription description = TypeDescription.createStruct()
        .addField("timestamp", TypeDescription.createLong());
    if (!config.splitFileByDevice) {
      description.addField("deviceId", TypeDescription.createString());
    }

    for (int i = 0; i < schema.getFields().length; i++) {
      if (config.ignoreStrings && schema.getTypes()[i] == String.class) {
        continue;
      }
      description.addField(schema.getFields()[i], toORCDataType(schema.getTypes()[i]));
    }
    return description;
  }

  private TypeDescription genNonAlignedWriteSchema(Schema schema) {
    TypeDescription description = TypeDescription.createStruct()
        .addField("timestamp", TypeDescription.createLong());
    if (!config.splitFileByDevice) {
      description.addField("deviceId", TypeDescription.createString());
    }
    description.addField("measurementId", TypeDescription.createString());
    description.addField("value", TypeDescription.createString());

    return description;
  }

  private TypeDescription toORCDataType(Class<?> type) {
    if (type == Long.class) {
      return TypeDescription.createLong();
    }
    if (type == Double.class) {
      return TypeDescription.createDouble();
    }
    return TypeDescription.createString();
  }

  private String dataTypeString(Class<?> type) {
    if (type == Long.class) {
      return "bigint";
    }
    if (type == Double.class) {
      return "DOUBLE";
    }
    return "string";
  }

  private String getReadSchema(String field, Class<?> filedType) {
    if (config.useAlignedSeries) {
      return getAlignedReadSchema(field, filedType);
    } else {
      return getNonAlignedReadSchema();
    }
  }

  private String getAlignedReadSchema(String field, Class<?> filedType) {
    if (!config.splitFileByDevice) {
      return "struct<timestamp:bigint,deviceId:string," + field + ":" + dataTypeString(filedType)
          + ">";
    } else {
      return "struct<timestamp:bigint," + field + ":" + dataTypeString(filedType) + ">";
    }
  }

  private String getNonAlignedReadSchema() {
    if (!config.splitFileByDevice) {
      return "struct<timestamp:bigint,deviceId:string,measurementId:string,value:string>";
    } else {
      return "struct<timestamp:bigint,measurementId:string,value:string>";
    }
  }

  @Override
  public long count(String tagValue, String field, long startTime, long endTime) {

    long start = System.nanoTime();

    // todo add type in parameter and config
    String schema = getReadSchema(field, Double.class);
    try {
      Reader reader = OrcFile.createReader(new Path(tagToFilePath(tagValue)),
          OrcFile.readerOptions(new Configuration()));
      TypeDescription readSchema = TypeDescription.fromString(schema);

      VectorizedRowBatch batch = readSchema.createRowBatch();
      RecordReader rowIterator = reader.rows(reader.options().schema(readSchema));

      long result = 0;
      while (rowIterator.nextBatch(batch)) {
        for (int r = 0; r < batch.size; ++r) {

          // time, deviceId, field
          long t = ((LongColumnVector) batch.cols[0]).vector[r];
          if (t < startTime || t > endTime) {
            continue;
          }
          if (config.useAlignedSeries) {
            result += countAligned(batch, tagValue, r);
          } else {
            result += countNonAligned(batch, tagValue, r, field);
          }
        }
      }
      rowIterator.close();

      logger.info("ORC result: {}", result);

    } catch (IOException e) {
      e.printStackTrace();
    }

    return System.nanoTime() - start;
  }

  private long countAligned(VectorizedRowBatch batch, String tagValue, int rowIndex) {
    long result = 0;
    if (!config.splitFileByDevice) {
      String deviceId = ((BytesColumnVector) batch.cols[1]).toString(rowIndex);
      if (deviceId.endsWith(tagValue)) {
        result++;
      }
      // double fieldValue = ((DoubleColumnVector) batch.cols[2]).vector[r];
    } else {
      result++;
      // double fieldValue = ((DoubleColumnVector) batch.cols[1]).vector[r];
    }
    return result;
  }

  private long countNonAligned(VectorizedRowBatch batch, String tagValue, int rowIndex,
      String field) {
    long result = 0;
    if (!config.splitFileByDevice) {
      String deviceId = ((BytesColumnVector) batch.cols[1]).toString(rowIndex);
      String measurementId = ((BytesColumnVector) batch.cols[2]).toString(rowIndex);
      if (deviceId != null && deviceId.endsWith(tagValue) && measurementId != null && measurementId.equals(field)) {
        result++;
      }
    } else {
      String measurementId = ((BytesColumnVector) batch.cols[1]).toString(rowIndex);
      if (measurementId != null && measurementId.equals(field)) {
        result++;
      }
    }
    return result;
  }

  @Override
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    long start = System.nanoTime();
    for (Entry<String, Writer> entry : writerMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      totalFileSize += new File(tagToFilePath(entry.getKey())).length();
    }
    logger.info("Total file size: {}", totalFileSize / (1024 * 1024.0));
    return System.nanoTime() - start;
  }
}
