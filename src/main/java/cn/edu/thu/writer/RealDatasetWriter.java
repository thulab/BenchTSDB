package cn.edu.thu.writer;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.RecordBatch;
import cn.edu.thu.common.Statistics;
import cn.edu.thu.database.*;
import cn.edu.thu.reader.BasicReader;
import cn.edu.thu.reader.CSVReader;
import cn.edu.thu.reader.GeolifeReader;
import cn.edu.thu.reader.NOAAReader;
import cn.edu.thu.reader.ReddReader;
import cn.edu.thu.reader.SyntheticReader;
import cn.edu.thu.reader.TDriveReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import backup.MLabUtilizationReader;

public class RealDatasetWriter implements Runnable {

  private static Logger logger = LoggerFactory.getLogger(RealDatasetWriter.class);
  private IDataBaseManager database;
  private Config config;
  private BasicReader reader;
  private final Statistics statistics;

  public RealDatasetWriter(Config config, List<String> files, final Statistics statistics) {
    this.database = DatabaseFactory.getDbManager(config);
    database.initClient();
    this.config = config;
    this.statistics = statistics;

    logger.info("thread construct!, need to read {} files", files.size());

    switch (config.DATA_SET) {
      case "NOAA":
        reader = new NOAAReader(config, files);
        break;
      case "GEOLIFE":
        reader = new GeolifeReader(config, files);
        break;
      case "TDRIVE":
        reader = new TDriveReader(config, files);
        break;
      case "MLAB_UTILIZATION":
        reader = new MLabUtilizationReader(config, files);
        break;
      case "REDD":
        reader = new ReddReader(config, files);
        break;
      case "SYNTHETIC":
        reader = new SyntheticReader(config);
        break;
      case "CSV":
        reader = new CSVReader(config, files);
        break;
      default:
        throw new RuntimeException(config.DATA_SET + " not supported");
    }

  }

  @Override
  public void run() {

    try {

      while(reader.hasNext()) {
        RecordBatch batch = reader.next();
        statistics.timeCost.addAndGet(database.insertBatch(batch, reader.getCurrentSchema()));
        statistics.recordNum.addAndGet(batch.size());
        statistics.nonNullPointNum.addAndGet(batch.getNonNullFieldNum());
        statistics.allPointNum.addAndGet(batch.getAllFieldNum());
      }
      reader.close();

      statistics.timeCost.addAndGet(database.flush());
      statistics.timeCost.addAndGet(database.close());

    } catch (Exception e) {
      logger.warn("Exception during write", e);
    }

  }

}
