/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.contrib.parser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.codehaus.jettison.json.JSONException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class RegexSplitterTest
{
  RegexSplitter regex = new RegexSplitter();
  private CollectorTestSink<Object> error = new CollectorTestSink<Object>();
  private CollectorTestSink<Object> pojoPort = new CollectorTestSink<Object>();

  @Rule
  public Watcher watcher = new Watcher();

  public class Watcher extends TestWatcher
  {
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      regex.err.setSink(error);
      regex.out.setSink(pojoPort);
      regex.setSchema(SchemaUtils.jarResourceFileToString("RegexSplitterschema.json"));
      regex.setSplitRegexPattern(".+\\[SEQ=\\w+\\]\\s*(\\d+:[\\d\\d:]+)\\s*(\\d+)\\s*(.+)");
      regex.setClazz(ServerLog.class);
      regex.setup(null);
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
    }

  }


  @Test
  public void TestValidInputCase() throws ParseException
  {
    regex.beginWindow(0);
    String line = "2015-10-01T03:14:49.000-07:00 lvn-d1-dev DevServer[9876]: INFO: [EVENT][SEQ=248717]  2015:10:01:03:14:49 101 sign-in_id=11111@psop.com ip_address=1.1.1.1  service_id=IP1234-NPB12345_00  result=RESULT_SUCCESconsole_id=0000000138e91b4e58236bf32besdafasdfasdfasdfsadf  account_id=11111  platform=pik";
    regex.processTuple(line.getBytes());
    regex.endWindow();
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(ServerLog.class, obj.getClass());
    ServerLog pojo = (ServerLog)obj;
    Assert.assertEquals(101, pojo.getId());
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy:MM:dd:hh:mm:ss");
    Date date = sdf.parse("2015:10:01:03:14:49");
    Assert.assertEquals(date, pojo.getDate());
    Assert.assertEquals("sign-in_id=11111@psop.com ip_address=1.1.1.1  service_id=IP1234-NPB12345_00  result=RESULT_SUCCESconsole_id=0000000138e91b4e58236bf32besdafasdfasdfasdfsadf  account_id=11111  platform=pik", pojo.getMessage());
  }

  @Test
  public void testEmptyInput() throws JSONException
  {
    String tuple = "";
    regex.beginWindow(0);
    regex.in.process(tuple.getBytes());
    regex.endWindow();
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestInValidInputCase() throws ParseException
  {
    regex.beginWindow(0);
    String line = "2015-10-01T03:14:49.000-07:00 lvn-d1-dev DevServer[9876]: INFO: [EVENT][SEQ=248717]  qwerty 101 sign-in_id=11111@psop.com ip_address=1.1.1.1  service_id=IP1234-NPB12345_00  result=RESULT_SUCCESconsole_id=0000000138e91b4e58236bf32besdafasdfasdfasdfsadf  account_id=11111  platform=pik";
    regex.processTuple(line.getBytes());
    regex.endWindow();
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(ServerLog.class, obj.getClass());
    ServerLog pojo = (ServerLog)obj;
    Assert.assertEquals(0, pojo.getId());
    Assert.assertEquals(null, pojo.getDate());
    Assert.assertEquals(null, pojo.getMessage());
  }

  @Test
  public void testNullInput() throws JSONException
  {
    regex.beginWindow(0);
    regex.in.process(null);
    regex.endWindow();
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }


  public static class ServerLog {
    private String message;
    private Date date;
    private int id;

    public String getMessage()
    {
      return message;
    }

    public void setMessage(String message)
    {
      this.message = message;
    }

    public int getId()
    {
      return id;
    }

    public void setId(int id)
    {
      this.id = id;
    }

    public Date getDate()
    {
      return date;
    }

    public void setDate(Date date)
    {
      this.date = date;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(RegexSplitterTest.class);
}
