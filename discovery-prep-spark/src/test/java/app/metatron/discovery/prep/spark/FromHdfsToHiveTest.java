/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.metatron.discovery.prep.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FromHdfsToHiveTest {

  static ObjectMapper mapper;

  static String getResourcePath(String relPath, boolean fromHdfs) {
    if (fromHdfs) {
      throw new IllegalArgumentException("HDFS not supported yet");
    }
    URL url = FromHdfsToHiveTest.class.getClassLoader().getResource(relPath);
    return (new File(url.getFile())).getAbsolutePath();
  }

  static String getResourcePath(String relPath) {
    return getResourcePath(relPath, false);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    mapper = new ObjectMapper();
  }

  String buildJsonPrepPropertiesInfo() throws JsonProcessingException {

    //////// prepPropertiesInfo
    Map<String, Object> prepPropertiesInfo = new HashMap();
    prepPropertiesInfo.put("spark.master", "local");
    prepPropertiesInfo.put("spark.appname", "spark_prep");
    prepPropertiesInfo.put("spark.driver.host", "localhost");
    prepPropertiesInfo.put("hive.metastore.uris", "thrift://localhost:9083");
    prepPropertiesInfo.put("polaris.dataprep.etl.limitRows", 1000000);
    prepPropertiesInfo.put("polaris.dataprep.etl.cores", 0);
    prepPropertiesInfo.put("polaris.dataprep.etl.timeout", 86400);

    return mapper.writeValueAsString(prepPropertiesInfo);
  }

  String buildJsonDatasetInfo(List<String> ruleStrings) throws JsonProcessingException {
    Map<String, Object> datasetInfo = new HashMap();


    datasetInfo.put("importType", "FILE");
    datasetInfo.put("delimiter", ",");

    // @TODO put into HDFS before test
    datasetInfo.put("filePath", "hdfs://localhost:9000/tmp/test/crime.csv");

    datasetInfo.put("upstreamDatasetInfos", new ArrayList());
    datasetInfo.put("origTeddyDsId", "a74f9474-4633-425f-88ea-5a33d543c84c");

    datasetInfo.put("ruleStrings", ruleStrings);

    return mapper.writeValueAsString(datasetInfo);
  }

  String buildJsonSnapshotInfo() throws JsonProcessingException {
    Map<String, Object> snapshotInfo = new HashMap();

    snapshotInfo.put("ssType", "HIVE");
    snapshotInfo.put("engine", "SPARK");
    snapshotInfo.put("compression", "NONE");
    snapshotInfo.put("ssName", "crime_hdfs"); // 저장 테이블 명
    snapshotInfo.put("ssId", "6e3eec52-fc60-4309-b0de-a53f93e08ce9");
    //snapshotInfo.put("mode", "APPEND");

    return mapper.writeValueAsString(snapshotInfo);
  }

  String buildJsonCallbackInfo() throws JsonProcessingException {
    Map<String, Object> callbackInfo = new HashMap();

    callbackInfo.put("port", 8180);
    callbackInfo.put("oauthToken", "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1MzYyNTY4NTEsInVzZXJfbmFtZSI6InBvbGFyaXMiLCJhdXRob3JpdGllcyI6WyJQRVJNX1NZU1RFTV9NQU5BR0VfU0hBUkVEX1dPUktTUEFDRSIsIl9fU0hBUkVEX1VTRVIiLCJQRVJNX1NZU1RFTV9NQU5BR0VfREFUQVNPVVJDRSIsIlBFUk1fU1lTVEVNX01BTkFHRV9QUklWQVRFX1dPUktTUEFDRSIsIlBFUk1fU1lTVEVNX1ZJRVdfV09SS1NQQUNFIiwiX19EQVRBX01BTkFHRVIiLCJfX1BSSVZBVEVfVVNFUiJdLCJqdGkiOiI3MzYxZjU2MS00MjVmLTQzM2ItOGYxZC01Y2RmOTlhM2RkMWIiLCJjbGllbnRfaWQiOiJwb2xhcmlzX2NsaWVudCIsInNjb3BlIjpbIndyaXRlIl19.iig9SBPrNUXoHp2wxGgZczfwt71fu3RBuRc14HxYxvg");

    return mapper.writeValueAsString(callbackInfo);
  }

  void testCrime(List<String> ruleStrings) throws JsonProcessingException {
    List<String> args = new ArrayList();

    args.add(buildJsonPrepPropertiesInfo());
    args.add(buildJsonDatasetInfo(ruleStrings));
    args.add(buildJsonSnapshotInfo());
    args.add(buildJsonCallbackInfo());

    Main.javaCall(args);
  }

  @Test
  public void testRule() throws JsonProcessingException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");

    ruleStrings.add("rename col: Population_ to: Population");
    ruleStrings.add("settype col: Population type: long ");
    ruleStrings.add("settype col: Date type: timestamp format: 'yyyy.MM.dd'");
    ruleStrings.add("drop col: Murder_,Forcible_Rape_ ");
    ruleStrings.add("keep row: Population>302173 ");

    testCrime(ruleStrings);
  }

}
