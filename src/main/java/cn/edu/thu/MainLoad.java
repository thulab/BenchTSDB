package cn.edu.thu;

import cn.edu.thu.common.BenchmarkExceptionHandler;
import cn.edu.thu.writer.RealDatasetWriter;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.Statistics;
import cn.edu.thu.database.*;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.concurrent.*;


public class MainLoad {

  private static Logger logger = LoggerFactory.getLogger(MainLoad.class);

  public static void main(String[] args) throws InterruptedException {

    if (args == null || args.length == 0) {
      args = new String[]{"conf/config.properties"};
    }

    final Statistics statistics = new Statistics();

    Config config;
    if (args.length > 0) {
      try {
        FileInputStream fileInputStream = new FileInputStream(args[0]);
        config = new Config(fileInputStream);
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("Load config from {} failed, using default config", args[0]);
        config = new Config();
      }
    } else {
      config = new Config();
    }

    // init database
    IDataBaseManager database = DatabaseFactory.getDbManager(config);
    database.initServer();

    logger.info("thread num : {}", config.THREAD_NUM);
    logger.info("using database: {}", config.DATABASE);

    File dirFile = new File(config.DATA_DIR);
    if (!dirFile.exists()) {
      logger.error(config.DATA_DIR + " do not exit");
      return;
    }

    List<String> files = new ArrayList<>();
    getAllFiles(config.DATA_DIR, files);
    logger.info("total files: {}", files.size());
    statistics.fileNum.addAndGet(files.size());

    Collections.sort(files);
    files = files.subList(config.BEGIN_FILE - 1, config.END_FILE);
    files.stream().map(File::new).map(File::length).reduce(Long::sum)
        .ifPresent(sum -> logger.info("Total file size: {} MB", sum * 1.0 / 1024 / 1024));

    List<List<String>> thread_files = new ArrayList<>();
    for (int i = 0; i < config.THREAD_NUM; i++) {
      thread_files.add(new ArrayList<>());
    }

    for (int i = 0; i < files.size(); i++) {
      if (config.useSynthetic) {
        break;
      }

//      if (i < config.BEGIN_FILE || i > config.END_FILE) {
//        continue;
//      }

      String filePath = files.get(i);
      if (filePath.contains(".DS_Store")) {
        continue;
      }
      int thread = i % config.THREAD_NUM;
      thread_files.get(thread).add(filePath);
    }

    Thread.UncaughtExceptionHandler handler = new BenchmarkExceptionHandler();
    ExecutorService executorService = Executors.newFixedThreadPool(config.THREAD_NUM);
    for (int threadId = 0; threadId < config.THREAD_NUM; threadId++) {
      Thread thread = new Thread(
          new RealDatasetWriter(config, thread_files.get(threadId), statistics));
      thread.setUncaughtExceptionHandler(handler);
      executorService.submit(thread);
    }

    executorService.shutdown();
    logger.info("@+++<<<: shutdown thread pool");

    // wait for all threads done
    boolean allDown = false;
    while (!allDown) {
      if (executorService.isTerminated()) {
        allDown = true;
      }
      Thread.sleep(1000);
    }

    logger.info("All done! Total records:{}, non-null points:{}, time:{} ms, speed:{}, null "
            + "ratio: {}",
        statistics.recordNum,
        statistics.nonNullPointNum, (float) statistics.timeCost.get() / 1000_000F,
        statistics.speed(),
        statistics.nonNullPointNum.get() * 1.0 / statistics.allPointNum.get());

  }

  private static void getAllFiles(String strPath, List<String> files) {
    File f = new File(strPath);
    if (f.isDirectory()) {
      File[] fs = f.listFiles();
      for (File f1 : fs) {
        String fsPath = f1.getAbsolutePath();
        getAllFiles(fsPath, files);
      }
    } else if (f.isFile()) {
      files.add(f.getAbsolutePath());
    }
  }

}
