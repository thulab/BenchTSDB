package cn.edu.thu.datasource;

import cn.edu.thu.common.Record;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.Statistics;
import cn.edu.thu.database.*;
import cn.edu.thu.datasource.parser.GeolifeParser;
import cn.edu.thu.datasource.parser.IParser;
import cn.edu.thu.datasource.parser.MLabIPParser;
import cn.edu.thu.datasource.parser.MLabUtilizationParser;
import cn.edu.thu.datasource.parser.NOAAParser;
import cn.edu.thu.datasource.parser.TDriveParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

public class FileReaderThread implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(FileReaderThread.class);
    private IDataBaseM database;
    private Config config;
    private int threadId;
    private IParser parser;
    private final Statistics statistics;

    public FileReaderThread(IDataBaseM database, Config config, int threadId, final Statistics statistics) {
        this.database = database;
        this.config = config;
        this.threadId = threadId;
        this.statistics = statistics;

        switch (config.DATA_SET) {
            case "NOAA":
                parser = new NOAAParser();
                break;
            case "GEOLIFE":
                parser = new GeolifeParser();
                break;
            case "TDRIVE":
                parser = new TDriveParser();
                break;
            case "MLAB_IP":
                parser = new MLabIPParser(config);
                break;
            case "MLAB_UTILIZATION":
                parser = new MLabUtilizationParser();
                break;
            default:
                throw new RuntimeException(config.DATA_SET + " not supported");
        }

    }

    @Override
    public void run() {

        logger.info("thread running!");

        long totalTime = 1;

        try {
            File dirFile = new File(config.DATA_DIR);
            if (!dirFile.exists()) {
                logger.error(config.DATA_DIR + " do not exit");
                return;
            }

            List<String> files = new ArrayList<>();
            getAllFiles(config.DATA_DIR, files);

            Collections.sort(files);

            long lineNum = 0;
            int fileNum = 0;

            logger.info("total file num: {}", files.size());

            for (int i = 0; i < files.size(); i++) {

                if(i< config.BEGINE_FILE || i > config.END_FILE) {
                    continue;
                }

                String filePath = files.get(i);

                // only read the file that can be exacted division by threadid
                if (i % config.THREAD_NUM != threadId) {
                    continue;
                }

                fileNum++;

                List<Record> records = parser.parse(filePath);

                lineNum += records.size();

                // write all data in this file to database
                long timecost = database.insertBatch(records);

                totalTime += timecost;

                logger.debug("processed the {}-th file in : {} ms", i, timecost);
            }

            totalTime += database.flush();

            logger.info("INFO! total produce {} files and {} lines", fileNum, lineNum);

            synchronized (statistics) {
                statistics.fileNum += fileNum;
                statistics.timeCost += totalTime;
                statistics.lineNum += lineNum;
                statistics.pointNum += lineNum * config.FIELDS.length;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private void getAllFiles(String strPath, List<String> files) {
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
