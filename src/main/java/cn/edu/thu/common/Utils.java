package cn.edu.thu.common;

import java.io.File;
import java.util.List;

public class Utils {

  public static void getAllFiles(String strPath, List<String> files) {
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

  public static int fieldSize(Object field) {
    if (field == null) {
      return 0;
    }
    if (field instanceof Double) {
      return Double.BYTES;
    }
    if (field instanceof Long) {
      return Long.BYTES;
    }
    return ((String) field).getBytes().length;
  }

  public static String removeQuote(String s) {
    if (isQuoted(s)) {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }

  public static boolean isQuoted(String s) {
    return s.length() >= 2 &&
        (s.startsWith("'") && s.endsWith("'") ||
            s.startsWith("\"") && s.endsWith("\""));
  }
}
