package cn.edu.thu.database.fileformat;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * time, seriesid, value
 *
 * time, deviceId, s1, s2, s3...
 *
 * time, series1, series2...
 */
public class ParquetManager implements IDataBaseManager {

  private static Logger logger = LoggerFactory.getLogger(ParquetManager.class);
  private Map<String, ParquetWriter> writerMap = new HashMap<>();
  private Map<String, SimpleGroupFactory> groupFactoryMap = new HashMap<>();
  private String lastTag;
  private Config config;
  private String filePath;
  private String schemaName = "defaultSchema";
  private long totalFileSize = 0;

  private boolean closeOnTagChanged = true;

  public ParquetManager(Config config) {
    this.config = config;
    this.filePath = config.FILE_PATH;
  }

  public ParquetManager(Config config, int threadNum) {
    this.config = config;
    this.filePath = config.FILE_PATH + "_" + threadNum;
  }

  @Override
  public void initServer() {

  }

  @Override
  public void initClient() {

  }

  private MessageType toParquetSchema(Schema schema) {
    Types.MessageTypeBuilder builder = Types.buildMessage();
    builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, Config.TIME_NAME));
    if (!config.splitFileByDevice) {
      builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, Config.TAG_NAME));
    }
    for (int i = 0; i < schema.getFields().length; i++) {
      builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL,
          toTypeName(schema.getTypes()[i]), schema.getFields()[i]));
    }

    return builder.named(schemaName);
  }

  private PrimitiveType.PrimitiveTypeName toTypeName(Class<?> type) {
    if (type == Long.class) {
      return PrimitiveTypeName.INT64;
    }
    if (type == Double.class) {
      return PrimitiveTypeName.DOUBLE;
    }
    return PrimitiveTypeName.BINARY;
  }

  private String tagToFilePath(String tag) {
    if (config.splitFileByDevice) {
      return filePath + "_" + tag;
    } else {
      return filePath + "_" + Config.DEFAULT_TAG;
    }
  }

  private ParquetWriter createWriter(String tag, Schema schema) {
    Configuration configuration = new Configuration();
    MessageType messageType = toParquetSchema(schema);
    GroupWriteSupport.setSchema(messageType, configuration);
    GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
    groupWriteSupport.init(configuration);

    String filePath = tagToFilePath(tag);
    new File(filePath).delete();
    try {
      groupFactoryMap.put(tag, new SimpleGroupFactory(messageType));
      logger.info("Created a writer for {}", tag);
      return new ParquetWriter(new Path(filePath), groupWriteSupport,
          CompressionCodecName.SNAPPY,
          ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
          true, true, ParquetProperties.WriterVersion.PARQUET_2_0);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  private ParquetWriter getWriter(String tag, Schema schema) {
    if (!config.splitFileByDevice) {
      return writerMap.computeIfAbsent(Config.DEFAULT_TAG, t -> createWriter(t, schema));
    } else {
      return writerMap.computeIfAbsent(tag, t -> createWriter(t, schema));
    }
  }


  @Override
  public long insertBatch(RecordBatch records, Schema schema) {
    long start = System.nanoTime();
    String tag = records.get(0).tag;
    if (closeOnTagChanged && config.splitFileByDevice && !Objects.equals(tag, lastTag)) {
      close();
    }

    ParquetWriter writer = getWriter(tag, schema);
    List<Group> groups = convertRecords(records, schema);
    for(Group group: groups) {
      try {
        writer.write(group);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    lastTag = tag;
    return System.nanoTime() - start;
  }


  private List<Group> convertRecords(List<Record> records, Schema schema) {
    List<Group> groups = new ArrayList<>();
    SimpleGroupFactory simpleGroupFactory = config.splitFileByDevice ?
        groupFactoryMap.get(records.get(0).tag) : groupFactoryMap.get(Config.DEFAULT_TAG);
    for(Record record: records) {
      Group group = simpleGroupFactory.newGroup();
      group.add(Config.TIME_NAME, record.timestamp);
      if (!config.splitFileByDevice) {
        group.add(Config.TAG_NAME, record.tag);
      }

      for(int i = 0; i < schema.getFields().length; i++) {
        writeColumn(group, record.fields.get(i), schema.getFields()[i], schema.getTypes()[i]);
      }
      groups.add(group);
    }
    return groups;
  }

  private void writeColumn(Group group, Object field, String fieldName, Class<?> type) {
    if (field != null) {
      if (type == Double.class) {
        double floatV = (double) field;
        group.add(fieldName, floatV);
      } else if (type == Long.class) {
        long longV = (long) field;
        group.add(fieldName, longV);
      } else {
        group.add(fieldName, field.toString());
      }
    }
  }

  @Override
  public long count(String tagValue, String field, long startTime, long endTime) {

    Configuration conf = new Configuration();
    if (!config.splitFileByDevice) {
      ParquetInputFormat.setFilterPredicate(conf, and(and(gtEq(longColumn(Config.TIME_NAME), startTime),
          ltEq(longColumn(Config.TIME_NAME), endTime)),
          eq(binaryColumn(Config.TAG_NAME), Binary.fromString(tagValue))));
    } else {
      ParquetInputFormat.setFilterPredicate(conf, and(gtEq(longColumn(Config.TIME_NAME), startTime),
          ltEq(longColumn(Config.TIME_NAME), endTime)));
    }

    FilterCompat.Filter filter = ParquetInputFormat.getFilter(conf);

    Types.MessageTypeBuilder builder = Types.buildMessage();
    builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, Config.TIME_NAME));
    if (!config.splitFileByDevice) {
      builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, Config.TAG_NAME));
    }
    // todo add field type
    builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, field));

    MessageType querySchema = builder.named(schemaName);
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, querySchema.toString());

    // set reader
    ParquetReader.Builder<Group> reader= ParquetReader
            .builder(new GroupReadSupport(), new Path(tagToFilePath(tagValue)))
            .withConf(conf)
            .withFilter(filter);

    long start = System.nanoTime();

    ParquetReader<Group> build;
    int result = 0;
    try {
      build = reader.build();
      Group line;
      while((line=build.read())!=null) {
        result++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    logger.info("Parquet result: {}", result);

    return System.nanoTime() - start;
  }

  @Override
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    long start = System.nanoTime();

    for (Entry<String, ParquetWriter> entry : writerMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      totalFileSize += new File(tagToFilePath(entry.getKey())).length();
    }
    logger.info("Total file size: {}", totalFileSize / (1024*1024.0));
    return System.nanoTime() - start;
  }
}
