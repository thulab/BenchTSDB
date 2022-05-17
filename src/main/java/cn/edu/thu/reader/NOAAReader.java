package cn.edu.thu.reader;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;

import cn.edu.thu.common.RecordBatch;
import cn.edu.thu.common.Schema;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class NOAAReader extends BasicReader {

  public static final Schema SCHEMA = new Schema(new String[]{"TEMP", "DEWP", "SLP", "STP",
      "VISIB", "WDSP", "MXSPD", "GUST", "MAX", "MIN", "PRCP", "SNDP", "FRSHTT"},
      new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 0});
  private DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

  public NOAAReader(Config config, List<String> files) {
    super(config, files);
  }


  private Record convertToRecord(String line) {

    try {

      List<Object> fields = new ArrayList<>();

      String tag = line.substring(0, 6).trim() + "_" + line.substring(7, 12).trim();
      //add 70 years, make sure time > 0
      String yearmoda = line.substring(14, 22).trim();
      Date date = dateFormat.parse(yearmoda);
      long time = date.getTime() + 2209046400000L;

      fields.add(Double.parseDouble(line.substring(24, 30).trim()));
      fields.add(Double.parseDouble(line.substring(35, 41).trim()));
      fields.add(Double.parseDouble(line.substring(46, 52).trim()));
      fields.add(Double.parseDouble(line.substring(57, 63).trim()));
      fields.add(Double.parseDouble(line.substring(68, 73).trim()));
      fields.add(Double.parseDouble(line.substring(78, 83).trim()));
      fields.add(Double.parseDouble(line.substring(88, 93).trim()));
      fields.add(Double.parseDouble(line.substring(95, 100).trim()));
      fields.add(Double.parseDouble(line.substring(102, 108).trim()));
      fields.add(Double.parseDouble(line.substring(110, 116).trim()));
      fields.add(Double.parseDouble(line.substring(118, 123).trim()));
      fields.add(Double.parseDouble(line.substring(125, 130).trim()));
      fields.add(Double.parseDouble(line.substring(132, 138).trim()));

      return new Record(time, currentDeviceId, fields);
    } catch (Exception ingore) {
      return null;
    }
  }

  @Override
  public void onFileOpened() throws Exception {
    String[] splitStrings = currentFile.split(config.DATA_DIR)[1].replaceAll("\\.op", "")
        .split("-");
    currentDeviceId = DEVICE_PREFIX + splitStrings[0] + "_" + splitStrings[1];

    // skip first line, which is the metadata
    reader.readLine();
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
