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

package org.apache.iotdb.db.http.service;

import java.time.ZonedDateTime;
import java.util.List;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.http.model.TimeValues;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class HttpService {
  private static final Logger logger = LoggerFactory.getLogger(HttpService.class);
  private HttpBackEnd httpBackEnd = new HttpBackEnd();

  public List<TimeValues> querySeries(String s, Pair<ZonedDateTime, ZonedDateTime> timeRange) {
    Long from = zonedCovertToLong(timeRange.left);
    Long to = zonedCovertToLong(timeRange.right);
    String sql = "SELECT " + s.substring(s.lastIndexOf('.') + 1) + " FROM root."
        + s.substring(0, s.lastIndexOf('.')) + " WHERE time > " + from + " and time < " + to;
    logger.info(sql);
    List<TimeValues> rows = null;
    try {
      rows = httpBackEnd.executeDataQuery(sql);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return rows;
  }

  public List<String> getMetaData() throws PathException {
    return httpBackEnd.getMetaData();
  }

  public void login(String username, String password) throws AuthException {
    httpBackEnd.login(username, password);
  }

  private Long zonedCovertToLong(ZonedDateTime time) {
    return time.toInstant().toEpochMilli();
  }

}
