package com.techlock.udfs;

import java.io.IOException;

import com.google.common.net.InetAddresses;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.SparkSession;

public class SanitizeIPUdf implements UDF1<String, String> {

  public SanitizeIPUdf () throws IOException {}

  /**
   * Simply try to parse as IP, if succesfull, return the ip, otherwise return null
   * 
   * @param ip
   * @return
   */
  private String sanitizeIP(String ip) {
    try {
      if (InetAddresses.isInetAddress(ip)) {
        return ip;
      } else {
        return null;
      }
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public String call(String arg) throws IOException {
    return sanitizeIP(arg);
  }

  public static void initSanitizeIP(SparkSession spark) throws IOException {
    spark.udf().register(
      "sanitize_ip",
      new SanitizeIPUdf(),
      DataTypes.StringType
    );
  }
}
