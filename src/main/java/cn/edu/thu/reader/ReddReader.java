package cn.edu.thu.reader;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.RecordBatch;
import cn.edu.thu.common.Schema;
import java.util.ArrayList;
import java.util.List;

public class ReddReader extends BasicReader {

  public static final Schema SCHEMA = new Schema(new String[]{"value"},
      new int[]{2});

  public ReddReader(Config config, List<String> files) {
    super(config, files);
  }

  @Override
  public void onFileOpened() {
    currentDeviceId = DEVICE_PREFIX + currentFile.split(config.DATA_DIR)[1].replaceAll("\\.dat", "")
        .replace("/", "_");
  }

  @Override
  public RecordBatch convertCachedLinesToRecords() {
    RecordBatch records = new RecordBatch();
    for (String line : cachedLines) {
      Record record = convertToRecord(line);
      if (record != null) {
        records.add(record);
      }
    }
    return records;
  }

  private Record convertToRecord(String line) {
    try {
      List<Object> fields = new ArrayList<>();
      String[] items = line.split(" ");
      long time = Long.parseLong(items[0]) * 1000;
      double value = Double.parseDouble(items[1]);
      fields.add(value);
      return new Record(time, currentDeviceId, fields);
    } catch (Exception ignore) {
    }
    return null;
  }

  @Override
  public Schema getCurrentSchema() {
    return SCHEMA;
  }
}
