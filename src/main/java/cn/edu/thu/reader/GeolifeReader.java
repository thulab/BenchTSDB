package cn.edu.thu.reader;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;

import cn.edu.thu.common.RecordBatch;
import cn.edu.thu.common.Schema;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeolifeReader extends BasicReader {

  public static final Schema SCHEMA = new Schema(new String[]{"Latitude", "Longitude", "Zero",
      "Altitude"}, new int[]{6, 6, 0, 0});

  private static Logger logger = LoggerFactory.getLogger(GeolifeReader.class);
  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh:mm:ss");

  public GeolifeReader(Config config, List<String> files) {
    super(config, files);
  }


  protected Record convertToRecord(String line) {
    try {
      List<Object> fields = new ArrayList<>();
      String[] items = line.split(",");

      fields.add(Double.parseDouble(items[0]));
      fields.add(Double.parseDouble(items[1]));
      fields.add(Double.parseDouble(items[2]));
      fields.add(Double.parseDouble(items[3]));

      Date date = dateFormat.parse(items[5] + "-" + items[6]);
      long time = date.getTime();
      return new Record(time, currentDeviceId, fields);
    } catch (Exception ignore) {
      logger.warn("can not parse: {}, error message: {}, File name: {}", line, ignore.getMessage(),
          files.get(currentFileIndex));
    }
    return null;
  }

  @Override
  public void onFileOpened() throws Exception {
    currentDeviceId = DEVICE_PREFIX + currentFile.split(config.DATA_DIR)[1].split("/Trajectory")[0];
    // skip 6 lines, which is useless
    for (int i = 0; i < 6; i++) {
      reader.readLine();
    }
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

  @Override
  public Schema getCurrentSchema() {
    return SCHEMA;
  }
}
