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
package org.apache.iotdb.db.http;

import org.apache.iotdb.db.http.controller.HttpController;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class HttpManager implements IService {

  private ConfigurableApplicationContext ctx;

  public static HttpManager getInstance() {
    return HttpManagerHolder.INSTANCE;
  }

  @Override
  public void start() {
    SpringApplication springApplication = new SpringApplication(HttpController.class);
    springApplication.setBannerMode(Banner.Mode.OFF);
    ctx = springApplication.run();
  }

  @Override
  public void stop() {
    ctx.close();
  }

  @Override
  public ServiceType getID() {
    return null;
  }

  private static class HttpManagerHolder {

    private static final HttpManager INSTANCE = new HttpManager();
  }
}
