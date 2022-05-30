package cn.edu.thu.database.fileformat;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.RecordBatch;
import cn.edu.thu.common.Schema;
import cn.edu.thu.database.IDataBaseManager;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.page.TimePageWriter;
import org.apache.iotdb.tsfile.write.page.ValuePageWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileManager implements IDataBaseManager {

  private static Logger logger = LoggerFactory.getLogger(TsFileManager.class);
  private Map<String, TsFileWriter> tagWriterMap = new HashMap<>();
  private Map<String, List<MeasurementSchema>> tagSchemasMap = new HashMap<>();
  private String lastTag;
  private String filePath;
  private Config config;
  private long totalFileSize;

  private boolean closeOnTagChanged = true;

  public TsFileManager(Config config) {
    this.config = config;
    this.filePath = config.FILE_PATH;
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(1024 * 1024 * 1024);
    TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(16 * 1024 * 1024);
  }

  public TsFileManager(Config config, int threadNum) {
    this.config = config;
    this.filePath = config.FILE_PATH + "_" + threadNum;
  }

  @Override
  public void initServer() {

  }

  @Override
  public void initClient() {
  }

//  @Override
//  public long insertBatch(List<Record> records) {
//    long start = System.nanoTime();
//    List<TSRecord> tsRecords = convertToRecords(records);
//    for (TSRecord tsRecord : tsRecords) {
//      try {
//        writer.write(tsRecord);
//      } catch (Exception e) {
//        e.printStackTrace();
//      }
//    }
//    return System.nanoTime() - start;
//  }

  private String tagToFilePath(String tag) {
    if (config.splitFileByDevice) {
      return filePath + "_" + tag;
    } else {
      return filePath + "_" + Config.DEFAULT_TAG;
    }
  }

  private TsFileWriter createWriter(String tag, Schema schema) {
    File file = new File(tagToFilePath(tag));
    file.getParentFile().mkdirs();
    TsFileWriter writer = null;
    try {
      writer = new TsFileWriter(file);
      Map<String, MeasurementSchema> template = new HashMap<>();
      List<MeasurementSchema> schemas = new ArrayList<>();

      for (int i = 0; i < schema.getFields().length; i++) {
        if (config.ignoreStrings && schema.getTypes()[i] == String.class) {
          continue;
        }

        Map<String, String> props = new HashMap<>();
        props.put(Encoder.MAX_POINT_NUMBER, schema.getPrecision()[i] + "");
        MeasurementSchema measurementSchema = new MeasurementSchema(schema.getFields()[i],
            toTsDataType(schema.getTypes()[i]),
            toTsEncoding(schema.getTypes()[i]), CompressionType.SNAPPY, props);
        template.put(schema.getFields()[i], measurementSchema);

        schemas.add(measurementSchema);
      }
      tagSchemasMap.put(tag, schemas);
      writer.registerSchemaTemplate(schema.getTag(), template, config.useAlignedSeries);
      logger.info("Created a writer of {}", tag);
      logger.debug("Writer schema {}", schema);
    } catch (Exception e) {
      logger.error("Cannot create writer with {}, schema {}", tag, schema);
    }

    return writer;
  }

  private TSDataType toTsDataType(Class<?> type) {
    if (type == Long.class) {
      return TSDataType.INT64;
    } else if (type == Double.class) {
      return TSDataType.DOUBLE;
    } else {
      return TSDataType.TEXT;
    }
  }

  private TSEncoding toTsEncoding(Class<?> type) {
    if (type == Long.class) {
      return toTsEncoding(config.longEncoding);
    } else if (type == Double.class) {
      return toTsEncoding(config.doubleEncoding);
    } else {
      return toTsEncoding(config.stringEncoding);
    }
  }

  private TSEncoding toTsEncoding(String encodingString) {
    for (TSEncoding encoding : TSEncoding.values()) {
      if (encoding.name().equalsIgnoreCase(encodingString)) {
        return encoding;
      }
    }
    logger.warn("Unknown encoding {}", encodingString);
    return TSEncoding.PLAIN;
  }

  private TsFileWriter getWriter(String tag, Schema schema) {
    if (!config.splitFileByDevice) {
      return tagWriterMap.computeIfAbsent(Config.DEFAULT_TAG, t -> createWriter(t, schema));
    } else {
      return tagWriterMap.computeIfAbsent(tag, t -> createWriter(t, schema));
    }
  }

  @Override
  public long insertBatch(RecordBatch records, Schema schema) {
    if (records.isEmpty()) {
      return 0;
    }

    long start = System.nanoTime();
    String tag = records.get(0).tag;
    if (closeOnTagChanged && config.splitFileByDevice && !Objects.equals(tag, lastTag)) {
      close();
    }

    TsFileWriter writer = getWriter(tag, schema);
    insertBatchAligned(records, writer, schema);

    lastTag = tag;
    return System.nanoTime() - start;
  }

//  private void insertBatchNonAligned(List<Record> records,
//      TsFileWriter writer, Schema schema) {
//    NonAlignedTablet tablet = convertToNonAlignedTablet(records, schema);
//    try {
//      writer.write(tablet);
//    } catch (Exception e) {
//      logger.error("Insert {} records failed, schema {}, ", records.size(), schema, e);
//    }
//  }

  private void insertBatchAligned(List<Record> records,
      TsFileWriter writer, Schema schema) {
    Tablet tablet = convertToTablet(records, schema);
    try {
      if (config.useAlignedSeries) {
        writer.writeAligned(tablet);
      } else {
        writer.write(tablet);
      }
    } catch (Exception e) {
      logger.error("Insert {} records failed, schema {}, ", records.size(), schema, e);
    }
  }

//  private NonAlignedTablet convertToNonAlignedTablet(List<Record> records,
//      Schema schema) {
//    String tag = records.get(0).tag;
//    List<MeasurementSchema> schemas = tagToMeasurementSchemas(tag);
//    NonAlignedTablet tablet = new NonAlignedTablet(tag, schemas,
//        records.size());
//    for (Record record: records) {
//      long timestamp = record.timestamp;
//      for (int i = 0; i < schema.getFields().length; i++) {
//        if (record.fields.get(i) != null) {
//          if (schema.getTypes()[i] != String.class) {
//            tablet.addValue(schemas.get(i).getMeasurementId(), timestamp, record.fields.get(i));
//          } else {
//            tablet.addValue(schemas.get(i).getMeasurementId(), timestamp,
//                new Binary((String) record.fields.get(i)));
//          }
//        }
//      }
//    }
//    return tablet;
//  }

  private List<MeasurementSchema> tagToMeasurementSchemas(String tag) {
    return tagSchemasMap.get(config.splitFileByDevice ? tag : Config.DEFAULT_TAG);
  }

  private Tablet convertToTablet(List<Record> records, Schema schema) {
    String tag = records.get(0).tag;
    Tablet tablet = new Tablet(tag, tagToMeasurementSchemas(tag), records.size());

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    tablet.bitMaps = new BitMap[values.length];
    for (int i = 0; i < tablet.bitMaps.length; i++) {
      tablet.bitMaps[i] = new BitMap(records.size());
    }

    for (Record record: records) {
      int row = tablet.rowSize++;
      timestamps[row] = record.timestamp;
      for (int i = 0; i < schema.getFields().length; i++) {
        if (config.ignoreStrings && schema.getTypes()[i] == String.class) {
          continue;
        }
        addToColumn(tablet.values[i], row, record.fields.get(i), tablet.bitMaps[i],
            schema.getTypes()[i]);
      }
    }
    return tablet;
  }

  private void addToColumn(Object column, int rowIndex, Object field, BitMap bitMap,
      Class<?> type) {
    if (type == Long.class) {
      addToLongColumn(column, rowIndex, field, bitMap);
    } else if (type == Double.class) {
      addToDoubleColumn(column, rowIndex, field, bitMap);
    } else {
      addToTextColumn(column, rowIndex, field, bitMap);
    }
  }

  private void addToDoubleColumn(Object column, int rowIndex, Object field, BitMap bitMap) {
    double[] sensor = (double[]) column;
    if (field != null) {
      sensor[rowIndex] = (double) field;
    } else {
      bitMap.mark(rowIndex);
    }
  }

  private void addToLongColumn(Object column, int rowIndex, Object field, BitMap bitMap) {
    long[] sensor = (long[]) column;
    if (field != null) {
      sensor[rowIndex] = (long) field;
    } else {
      bitMap.mark(rowIndex);
    }
  }

  private void addToTextColumn(Object column, int rowIndex, Object field, BitMap bitMap) {
    Binary[] sensor = (Binary[]) column;
    if (field != null) {
      sensor[rowIndex] = new Binary((String) field);
    } else {
      bitMap.mark(rowIndex);
    }
  }


  private List<TSRecord> convertToRecords(List<Record> records, Schema schema) {
    List<TSRecord> tsRecords = new ArrayList<>();
    for (Record record : records) {
      TSRecord tsRecord = new TSRecord(record.timestamp, record.tag);
      for (int i = 0; i < schema.getFields().length; i++) {
        double floatField = (double) record.fields.get(i);
        tsRecord.addTuple(new DoubleDataPoint(schema.getFields()[i], floatField));
      }
      tsRecords.add(tsRecord);
    }
    return tsRecords;
  }

  @Override
  public long count(String tagValue, String field, long startTime, long endTime) {

    long start = System.nanoTime();
    try (TsFileReader readTsFile = new TsFileReader(new TsFileSequenceReader(tagToFilePath(tagValue)))) {

      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path(tagValue, field));
      IExpression filter = new SingleSeriesExpression(new Path(tagValue, field),
          new AndFilter(TimeFilter.gtEq(startTime), TimeFilter.ltEq(endTime)));

      QueryExpression queryExpression = QueryExpression.create(paths, filter);

      QueryDataSet queryDataSet = readTsFile.query(queryExpression);

      int i = 0;
      while (queryDataSet.hasNext()) {
        i++;
        queryDataSet.next();
      }

      logger.info("TsFile count result: {}", i);
    } catch (IOException e) {
      logger.error("Cannot count", e);
    }

    return System.nanoTime() - start;
  }

  @Override
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    long start = System.nanoTime();

    for (Entry<String, TsFileWriter> entry : tagWriterMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        logger.warn("Close file failed: {}", entry.getKey());
      }
      totalFileSize += new File(tagToFilePath(entry.getKey())).length();
    }

    tagWriterMap.clear();
    tagSchemasMap.clear();

    logger.info("Total file size: {}", totalFileSize / (1024*1024.0));
    reportCompression();
    return System.nanoTime() - start;
  }

  private void reportCompression() {
    long timeRawSize = PageWriter.timeRawSize.get() + TimePageWriter.timeRawSize.get();
    long timeEncodedSize = PageWriter.timeEncodedSize.get() + TimePageWriter.timeEncodedSize.get();
    long valueRawSize = PageWriter.valueRawSize.get() + ValuePageWriter.valueRawSize.get();
    long valueEncodedSize = PageWriter.valueEncodedSize.get() + ValuePageWriter.valueEncodedSize.get();
    long compressedSize =
        PageWriter.compressedSize.get() + ValuePageWriter.valueCompressedSize.get() + TimePageWriter.timeCompressedSize
            .get();
    logger.info("Time raw size: {}, encoding ratio: {}; value raw size: {}, encoding ratio: {}; "
            + "total compression ratio: {}",
        timeRawSize,
        timeEncodedSize * 1.0 / timeRawSize,
        valueRawSize,
        valueEncodedSize * 1.0 / valueRawSize,
        compressedSize * 1.0 / (timeRawSize + valueRawSize));
  }
}
