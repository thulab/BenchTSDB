package cn.edu.thu.reader;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.RecordBatch;
import cn.edu.thu.common.Schema;
import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TDriveReader extends BasicReader {

  public static final Schema SCHEMA = new Schema(new String[]{"longitude", "latitude"},
      new int[]{5, 5});
  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
  private static Logger logger = LoggerFactory.getLogger(TDriveReader.class);

  public TDriveReader(Config config, List<String> files) {
    super(config, files);
  }

  @Override
  public void onFileOpened() {
    currentDeviceId =
        DEVICE_PREFIX + currentFile.split(config.DATA_DIR)[1].replaceAll("\\.txt", "");
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

      String[] items = line.split(",");

      fields.add(Double.parseDouble(items[2]));
      fields.add(Double.parseDouble(items[3]));

      Date date = dateFormat.parse(items[1]);
      long time = date.getTime();

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
