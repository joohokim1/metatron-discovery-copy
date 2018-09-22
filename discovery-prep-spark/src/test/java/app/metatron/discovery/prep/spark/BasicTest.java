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

public class BasicTest {

  static ObjectMapper mapper;

  static String getResourcePath(String relPath, boolean fromHdfs) {
    if (fromHdfs) {
      throw new IllegalArgumentException("HDFS not supported yet");
    }
    URL url = BasicTest.class.getClassLoader().getResource(relPath);
    return (new File(url.getFile())).getAbsolutePath();
  }

  public static String getResourcePath(String relPath) {
    return getResourcePath(relPath, false);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    mapper = new ObjectMapper();
  }

  @Test
  public void testRename() throws JsonProcessingException {
    Map<String, Object> prepPropertiesInfo = new HashMap();
    Map<String, Object> datasetInfo = new HashMap();
    Map<String, Object> snapshotInfo = new HashMap();
    Map<String, Object> callbackInfo = new HashMap();

    prepPropertiesInfo.put("polaris.dataprep.etl.limitRows", 1000000);
    prepPropertiesInfo.put("polaris.dataprep.etl.cores", 0);
    prepPropertiesInfo.put("polaris.dataprep.etl.timeout", 86400);

    datasetInfo.put("importType", "FILE");
    datasetInfo.put("delimiter", ",");
    datasetInfo.put("filePath", getResourcePath("crime.csv"));   // put into HDFS before test
    datasetInfo.put("upstreamDatasetInfos", new ArrayList());
    datasetInfo.put("origTeddyDsId", "a74f9474-4633-425f-88ea-5a33d543c84c");

    List<String> ruleStrings = new ArrayList();
    ruleStrings.add("rename col: _c0 to: new_colname");
    datasetInfo.put("ruleStrings", ruleStrings);

    snapshotInfo.put("stagingBaseDir", "/test");
    snapshotInfo.put("ssType", "HDFS");
    snapshotInfo.put("engine", "SPARK");
    snapshotInfo.put("format", "CSV");
    snapshotInfo.put("compression", "NONE");
    snapshotInfo.put("ssName", "crime_20180913_053230");
    snapshotInfo.put("ssId", "6e3eec52-fc60-4309-b0de-a53f93e08ce9");

    callbackInfo.put("port", 8180);
    callbackInfo.put("oauthToken", "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1MzYyNTY4NTEsInVzZXJfbmFtZSI6InBvbGFyaXMiLCJhdXRob3JpdGllcyI6WyJQRVJNX1NZU1RFTV9NQU5BR0VfU0hBUkVEX1dPUktTUEFDRSIsIl9fU0hBUkVEX1VTRVIiLCJQRVJNX1NZU1RFTV9NQU5BR0VfREFUQVNPVVJDRSIsIlBFUk1fU1lTVEVNX01BTkFHRV9QUklWQVRFX1dPUktTUEFDRSIsIlBFUk1fU1lTVEVNX1ZJRVdfV09SS1NQQUNFIiwiX19EQVRBX01BTkFHRVIiLCJfX1BSSVZBVEVfVVNFUiJdLCJqdGkiOiI3MzYxZjU2MS00MjVmLTQzM2ItOGYxZC01Y2RmOTlhM2RkMWIiLCJjbGllbnRfaWQiOiJwb2xhcmlzX2NsaWVudCIsInNjb3BlIjpbIndyaXRlIl19.iig9SBPrNUXoHp2wxGgZczfwt71fu3RBuRc14HxYxvg");

    String jsonPrepPropertiesInfo = mapper.writeValueAsString(prepPropertiesInfo);
    String jsonDatasetInfo        = mapper.writeValueAsString(datasetInfo);
    String jsonSnapshotInfo       = mapper.writeValueAsString(snapshotInfo);
    String jsonCallbackInfo       = mapper.writeValueAsString(callbackInfo);

    List<String> args = new ArrayList();
    args.add(jsonPrepPropertiesInfo);
    args.add(jsonDatasetInfo);
    args.add(jsonSnapshotInfo);
    args.add(jsonCallbackInfo);

    Main.javaCall(args);
  }

  @Test
  public void testHeader() throws JsonProcessingException {
    Map<String, Object> prepPropertiesInfo = new HashMap();
    Map<String, Object> datasetInfo = new HashMap();
    Map<String, Object> snapshotInfo = new HashMap();
    Map<String, Object> callbackInfo = new HashMap();

    prepPropertiesInfo.put("polaris.dataprep.etl.limitRows", 1000000);
    prepPropertiesInfo.put("polaris.dataprep.etl.cores", 0);
    prepPropertiesInfo.put("polaris.dataprep.etl.timeout", 86400);

    datasetInfo.put("importType", "FILE");
    datasetInfo.put("delimiter", ",");
    datasetInfo.put("filePath", getResourcePath("crime.csv"));   // put into HDFS before test
    datasetInfo.put("upstreamDatasetInfos", new ArrayList());
    datasetInfo.put("origTeddyDsId", "a74f9474-4633-425f-88ea-5a33d543c84c");

    List<String> ruleStrings = new ArrayList();
    ruleStrings.add("header rownum: 1");
    datasetInfo.put("ruleStrings", ruleStrings);

    snapshotInfo.put("stagingBaseDir", "/test");
    snapshotInfo.put("ssType", "HDFS");
    snapshotInfo.put("engine", "SPARK");
    snapshotInfo.put("format", "CSV");
    snapshotInfo.put("compression", "NONE");
    snapshotInfo.put("ssName", "crime_20180913_053230");
    snapshotInfo.put("ssId", "6e3eec52-fc60-4309-b0de-a53f93e08ce9");

    callbackInfo.put("port", 8180);
    callbackInfo.put("oauthToken", "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1MzYyNTY4NTEsInVzZXJfbmFtZSI6InBvbGFyaXMiLCJhdXRob3JpdGllcyI6WyJQRVJNX1NZU1RFTV9NQU5BR0VfU0hBUkVEX1dPUktTUEFDRSIsIl9fU0hBUkVEX1VTRVIiLCJQRVJNX1NZU1RFTV9NQU5BR0VfREFUQVNPVVJDRSIsIlBFUk1fU1lTVEVNX01BTkFHRV9QUklWQVRFX1dPUktTUEFDRSIsIlBFUk1fU1lTVEVNX1ZJRVdfV09SS1NQQUNFIiwiX19EQVRBX01BTkFHRVIiLCJfX1BSSVZBVEVfVVNFUiJdLCJqdGkiOiI3MzYxZjU2MS00MjVmLTQzM2ItOGYxZC01Y2RmOTlhM2RkMWIiLCJjbGllbnRfaWQiOiJwb2xhcmlzX2NsaWVudCIsInNjb3BlIjpbIndyaXRlIl19.iig9SBPrNUXoHp2wxGgZczfwt71fu3RBuRc14HxYxvg");

    String jsonPrepPropertiesInfo = mapper.writeValueAsString(prepPropertiesInfo);
    String jsonDatasetInfo        = mapper.writeValueAsString(datasetInfo);
    String jsonSnapshotInfo       = mapper.writeValueAsString(snapshotInfo);
    String jsonCallbackInfo       = mapper.writeValueAsString(callbackInfo);

    List<String> args = new ArrayList();
    args.add(jsonPrepPropertiesInfo);
    args.add(jsonDatasetInfo);
    args.add(jsonSnapshotInfo);
    args.add(jsonCallbackInfo);

    Main.javaCall(args);
  }

  @Test
  public void testReplace() throws JsonProcessingException {
    Map<String, Object> prepPropertiesInfo = new HashMap();
    Map<String, Object> datasetInfo = new HashMap();
    Map<String, Object> snapshotInfo = new HashMap();
    Map<String, Object> callbackInfo = new HashMap();

    prepPropertiesInfo.put("polaris.dataprep.etl.limitRows", 1000000);
    prepPropertiesInfo.put("polaris.dataprep.etl.cores", 0);
    prepPropertiesInfo.put("polaris.dataprep.etl.timeout", 86400);

    datasetInfo.put("importType", "FILE");
    datasetInfo.put("delimiter", ",");
    datasetInfo.put("filePath", getResourcePath("crime.csv"));   // put into HDFS before test
    datasetInfo.put("upstreamDatasetInfos", new ArrayList());
    datasetInfo.put("origTeddyDsId", "a74f9474-4633-425f-88ea-5a33d543c84c");

    String colNames = "Population_, Total_Crime, Violent_Crime, Property_Crime, Murder_, Forcible_Rape_, Robbery_, Aggravated_Assault_, Burglary_, Larceny_Theft_, Vehicle_Theft_";
    String commonClauses = "global: true ignoreCase: false";

    List<String> ruleStrings = new ArrayList();
    ruleStrings.add("header rownum: 1");
    ruleStrings.add(String.format("replace col: %s with: '' on: '_' %s", colNames, commonClauses));
    ruleStrings.add(String.format("replace col: %s with: '' on: ',' %s", colNames, commonClauses));
    ruleStrings.add(String.format("replace col: %s with: '' on: ' ' %s", colNames, commonClauses));
    datasetInfo.put("ruleStrings", ruleStrings);

    snapshotInfo.put("stagingBaseDir", "/test");
    snapshotInfo.put("ssType", "HDFS");
    snapshotInfo.put("engine", "SPARK");
    snapshotInfo.put("format", "CSV");
    snapshotInfo.put("compression", "NONE");
    snapshotInfo.put("ssName", "crime_20180913_053230");
    snapshotInfo.put("ssId", "6e3eec52-fc60-4309-b0de-a53f93e08ce9");

    callbackInfo.put("port", 8180);
    callbackInfo.put("oauthToken", "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1MzYyNTY4NTEsInVzZXJfbmFtZSI6InBvbGFyaXMiLCJhdXRob3JpdGllcyI6WyJQRVJNX1NZU1RFTV9NQU5BR0VfU0hBUkVEX1dPUktTUEFDRSIsIl9fU0hBUkVEX1VTRVIiLCJQRVJNX1NZU1RFTV9NQU5BR0VfREFUQVNPVVJDRSIsIlBFUk1fU1lTVEVNX01BTkFHRV9QUklWQVRFX1dPUktTUEFDRSIsIlBFUk1fU1lTVEVNX1ZJRVdfV09SS1NQQUNFIiwiX19EQVRBX01BTkFHRVIiLCJfX1BSSVZBVEVfVVNFUiJdLCJqdGkiOiI3MzYxZjU2MS00MjVmLTQzM2ItOGYxZC01Y2RmOTlhM2RkMWIiLCJjbGllbnRfaWQiOiJwb2xhcmlzX2NsaWVudCIsInNjb3BlIjpbIndyaXRlIl19.iig9SBPrNUXoHp2wxGgZczfwt71fu3RBuRc14HxYxvg");

    String jsonPrepPropertiesInfo = mapper.writeValueAsString(prepPropertiesInfo);
    String jsonDatasetInfo        = mapper.writeValueAsString(datasetInfo);
    String jsonSnapshotInfo       = mapper.writeValueAsString(snapshotInfo);
    String jsonCallbackInfo       = mapper.writeValueAsString(callbackInfo);

    List<String> args = new ArrayList();
    args.add(jsonPrepPropertiesInfo);
    args.add(jsonDatasetInfo);
    args.add(jsonSnapshotInfo);
    args.add(jsonCallbackInfo);

    Main.javaCall(args);
  }
}
