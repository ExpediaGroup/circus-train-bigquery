package com.hotels.bdp.circustrain.bigquery.util;

import java.util.UUID;

public class BigQueryUriUtils {

  public static String randomUri() {
    return UUID.randomUUID().toString().toLowerCase();
  }

}
