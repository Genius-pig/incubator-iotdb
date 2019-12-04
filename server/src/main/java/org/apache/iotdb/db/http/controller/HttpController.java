/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.http.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.http.model.TimeValues;
import org.apache.iotdb.db.http.service.HttpService;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.*;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@EnableAutoConfiguration
@ComponentScan({"org.apache.iotdb.db.http.service"})
public class HttpController {

  private static final Logger logger = LoggerFactory.getLogger(HttpController.class);

  private final HttpService httpService;

  public HttpController(HttpService httpService) {
    this.httpService = httpService;
  }

  @RequestMapping(value = "/", method = RequestMethod.GET)
  @ResponseStatus(value = HttpStatus.OK)
  public void testDataConnection(HttpServletResponse response) throws IOException {
    logger.info("Connection is ok now!");
    response.getWriter().print("I have sent a message.");
  }

  @RequestMapping(value = "/login")
  @ResponseBody
  public void login(@RequestParam(value="username", defaultValue="root") String username,
      @RequestParam(value="password", defaultValue="root") String password) throws AuthException {
    httpService.login(username, password);
  }

  /**
   * get metrics numbers in JSON string structure.
   *
   * @param response http response
   * @return metrics numbers in JSON string structure
   */
  @RequestMapping(value = "/search")
  @ResponseBody
  public String metricFindQuery(HttpServletResponse response) {
    JSONObject jsonObject = new JSONObject();
    response.setStatus(200);
    List<String> columnsName = new ArrayList<>();
    try {
      columnsName = httpService.getMetaData();
    } catch (Exception e) {
      logger.error("Failed to get metadata", e);
    }
    Collections.sort(columnsName);
    for (int i = 0; i < columnsName.size(); i++) {
      jsonObject.put( i + "", columnsName.get(i));
    }
    return jsonObject.toString();
  }

  /**
   * convert query result data to JSON format.
   *
   * @param request http request
   * @param response http response
   * @return data in JSON format
   */
  @RequestMapping(value = "/query")
  @ResponseBody
  public String query(HttpServletRequest request, HttpServletResponse response) {
    String targetStr = "target";
    response.setStatus(200);
    try {
      JSONObject jsonObject = getRequestBodyJson(request);
      assert jsonObject != null;
      JSONObject range = (JSONObject) jsonObject.get("range");
      Pair<String, String> timeRange = new Pair<>((String) range.get("from"), (String) range.get("to"));
      JSONArray array = (JSONArray) jsonObject.get("targets"); // []
      JSONArray result = new JSONArray();
      for (int i = 0; i < array.size(); i++) {
        JSONObject object = (JSONObject) array.get(i); // {}
        if (!object.containsKey(targetStr)) {
          return "[]";
        }
        String target = (String) object.get(targetStr);
        String type = getJsonType(jsonObject);
        JSONObject obj = new JSONObject();
        obj.put("target", target);
        if (type.equals("table")) {
          setJsonTable(obj, target, timeRange);
        } else if (type.equals("timeserie")) {
          setJsonTimeseries(obj, target, timeRange);
        }
        result.add(i, obj);
      }
      logger.info("query finished");
      return result.toString();
    } catch (Exception e) {
      logger.error("/query failed", e);
    }
    return null;
  }

  private void setJsonTable(JSONObject obj, String target,
      Pair<String, String> timeRange)
      throws JSONException {
    List<TimeValues> timeValues = httpService.querySeries(target, timeRange);
    JSONArray columns = new JSONArray();
    JSONObject column = new JSONObject();
    column.put("text", "Time");
    column.put("type", "time");
    columns.add(column);
    column = new JSONObject();
    column.put("text", "Number");
    column.put("type", "number");
    columns.add(column);
    obj.put("columns", columns);
    JSONArray values = new JSONArray();
    for (TimeValues tv : timeValues) {
      JSONArray value = new JSONArray();
      value.add(tv.getTime());
      value.add(tv.getValue());
      values.add(value);
    }
    obj.put("values", values);
  }

  private void setJsonTimeseries(JSONObject obj, String target,
      Pair<String, String> timeRange)
      throws JSONException {
    List<TimeValues> timeValues = httpService.querySeries(target, timeRange);
    logger.info("query size: {}", timeValues.size());
    JSONArray dataPoints = new JSONArray();
    for (TimeValues tv : timeValues) {
      long time = tv.getTime();
      String value = tv.getValue();
      JSONArray jsonArray = new JSONArray();
      jsonArray.add(value);
      jsonArray.add(time);
      dataPoints.add(jsonArray);
    }
    obj.put("datapoints", dataPoints);
  }

  /**
   * get request body JSON.
   *
   * @param request http request
   * @return request JSON
   * @throws JSONException JSONException
   */
  private JSONObject getRequestBodyJson(HttpServletRequest request) throws JSONException {
    try {
      BufferedReader br = request.getReader();
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
      return JSON.parseObject(sb.toString());
    } catch (IOException e) {
      logger.error("getRequestBodyJson failed", e);
    }
    return null;
  }

  /**
   * get JSON type of input JSON object.
   *
   * @param jsonObject JSON Object
   * @return type (string)
   * @throws JSONException JSONException
   */
  private String getJsonType(JSONObject jsonObject) throws JSONException {
    JSONArray array = (JSONArray) jsonObject.get("targets"); // []
    JSONObject object = (JSONObject) array.get(0); // {}
    return (String) object.get("type");
  }
}
