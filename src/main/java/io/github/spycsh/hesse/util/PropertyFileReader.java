package io.github.spycsh.hesse.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFileReader {

  private static Properties prop = new Properties();

  public static Properties readPropertyFile() throws Exception {
    if (prop.isEmpty()) {
      InputStream input =
          PropertyFileReader.class.getClassLoader().getResourceAsStream("hesse.properties");
      try {
        prop.load(input);
      } catch (IOException ex) {
        System.out.println(ex);
        throw ex;
      } finally {
        if (input != null) {
          input.close();
        }
      }
    }

    return prop;
  }
}
