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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.storageGroup.StorageGroupException;
import org.apache.iotdb.db.http.model.TimeValues;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpBackEnd {

  protected String username;
  private static final String INFO_NOT_LOGIN = "{}: Not login.";
  private static final Logger logger = LoggerFactory.getLogger(HttpBackEnd.class);
  protected QueryProcessor processor = new QueryProcessor(new QueryProcessExecutor());


  void login(String username, String password) throws AuthException {
    logger.info("{}: receive open session request from username {}", IoTDBConstant.GLOBAL_DB_NAME,
        username);

    boolean status;
    IAuthorizer authorizer;
    try {
      authorizer = LocalFileAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new AuthException("meet error while logging in.");
    }
    try {
      assert authorizer != null;
      status = authorizer.login(username, password);
    } catch (AuthException e) {
      throw new AuthException("meet error while logging in.");
    }

    if (status) {
      this.username = username;
    } else {
      throw new AuthException("Wrong login password");
    }
    logger.info("{}: Login successfully. User : {}", IoTDBConstant.GLOBAL_DB_NAME, username);
  }


  List<TimeValues> executeDataQuery(String sql)
      throws AuthException, QueryProcessException,
      StorageGroupException, StorageEngineException,
      QueryFilterOptimizationException, IOException, MetadataException {
    QueryPlan plan = (QueryPlan) processor
        .parseSQLToPhysicalPlan(sql);
    List<Path> paths = plan.getPaths();
    if (!checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
    }

    // check seriesPath exists
    if (paths.isEmpty()) {
      throw new PathException("The path doesn't exist");
    }

    // check file level set
    try {
      checkFileLevelSet(paths);
    } catch (StorageGroupException e) {
      logger.error("meet error while checking file level.", e);
      throw new StorageGroupException(e.getMessage());
    }

    // check permissions
    if (!checkAuthorization(paths, plan)) {
      throw new AuthException("Don't have permissions");
    }

    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignJobId());
    QueryDataSet queryDataSet = processor.getExecutor().processQuery(plan, context);
    String[] args;
    List<TimeValues> list = new ArrayList<>();
    while(queryDataSet.hasNext()) {
      TimeValues timeValues = new TimeValues();
      args = queryDataSet.next().toString().split("\t");
      timeValues.setTime(Long.parseLong(args[1]));
      timeValues.setValue(args[0]);
      list.add(timeValues);
    }

    return list;
  }

  private void checkFileLevelSet(List<Path> paths) throws StorageGroupException {
    MManager.getInstance().checkFileLevel(paths);
  }

  private boolean checkAuthorization(List<Path> paths, PhysicalPlan plan) throws AuthException {
    return AuthorityChecker.check(username, paths, plan.getOperatorType(), null);
  }

  public List<String> getMetaData() throws PathException {
    List<List<String>> lists = getTimeSeriesForPath();
    List<String> paths = new ArrayList<>();
    for(List<String> list : lists) {
      paths.add(list.get(0).substring(5));
    }
    return paths;
  }

  private List<List<String>> getTimeSeriesForPath()
      throws PathException {
    return MManager.getInstance().getShowTimeseriesPath("root.*");
  }

  private boolean checkLogin() {
    return username != null;
  }

}
