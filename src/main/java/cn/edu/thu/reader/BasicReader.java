package cn.edu.thu.reader;

import cn.edu.thu.common.RecordBatch;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.Schema;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasicReader implements Iterator<RecordBatch> {

  public static final String DEVICE_PREFIX = "root.group_0.d_";

  private static Logger logger = LoggerFactory.getLogger(BasicReader.class);
  protected Config config;
  protected List<String> files;
  protected BufferedReader reader;
  protected List<String> cachedLines;
  protected int currentFileIndex = 0;
  protected int fileRowCnt;

  protected String currentFile;
  protected String currentDeviceId;

  protected ReaderStatistic stat = new ReaderStatistic();

  public BasicReader(Config config) {
    this.config = config;
  }

  public BasicReader(Config config, List<String> files) {
    this.config = config;
    this.files = files;
    cachedLines = new ArrayList<>(config.BATCH_SIZE);
    try {
      reader = new BufferedReader(new FileReader(files.get(currentFileIndex)));
      currentFile = files.get(currentFileIndex);
      logger.info("start to read {}-th file {}", currentFileIndex, currentFile);
      fileRowCnt = 0;
      onFileOpened();
    } catch (Exception e) {
      logger.error("meet exception when init file: {}", currentFile);
      e.printStackTrace();
    }
  }

  private void newFile() throws Exception {
    currentFile = files.get(++currentFileIndex);
    logger.info("start to read {}-th file {}", currentFileIndex, currentFile);
    reader.close();
    reader = new BufferedReader(new FileReader(currentFile));
    fileRowCnt = 0;
    onFileOpened();
  }

  public boolean hasNext() {

    if (!cachedLines.isEmpty()) {
      return true;
    }

    try {
      String line;
      while (true) {

        if(reader == null) {
          return false;
        }

        line = reader.readLine();

        // current file end
        if(line == null) {

          // current file has been resolved, read next file
          if(cachedLines.isEmpty()) {
            if (currentFileIndex < files.size() - 1) {
              newFile();
              continue;
            } else {
              // no more file to read
              reader.close();
              reader = null;
              break;
            }
          } else {
            // resolve current file
            return true;
          }
        } else if (line.isEmpty()){
          continue;
        }

        // read a line, cache it
        cachedLines.add(line);
        fileRowCnt ++;
        if (config.rowLimitPerFile > 0 && fileRowCnt >= config.rowLimitPerFile) {
          if (currentFileIndex < files.size() - 1) {
            newFile();
            continue;
          } else {
            // no more file to read
            reader.close();
            reader = null;
            break;
          }
        }
        if (cachedLines.size() >= config.BATCH_SIZE) {
          break;
        }
      }
    } catch (Exception e) {
      logger.error("read file {} failed", currentFile, e);
      return false;
    }

    return !cachedLines.isEmpty();
  }


  /**
   * convert the cachedLines to Record list
   */
  protected abstract RecordBatch convertCachedLinesToRecords();

  public RecordBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    RecordBatch records = convertCachedLinesToRecords();
    cachedLines.clear();

    return records;
  }

  /**
   * initialize when start reading a file
   * maybe skip the first lines
   * maybe init the tagValue(deviceId) from file name
   */
  public abstract void onFileOpened() throws Exception;

  public abstract Schema getCurrentSchema();

  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      cachedLines.clear();
      reader = null;
    }
    logger.info("Record reader closed.");
  }
}
