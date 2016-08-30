/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.contrib.parser;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConversionException;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.commons.beanutils.converters.DateTimeConverter;
import org.apache.commons.lang.StringUtils;

import com.datatorrent.api.Context;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Operator splits the tuple based on regex pattern and parses the matches against a specified schema <br>
 * This operator expects the upstream operator to send every line in the file as a tuple.
 * splitRegexPattern contains the regex pattern of lines in the file <br>
 * Schema is specified in a json format as per {@link DelimitedSchema} that
 * contains field information and constraints for each field.<br>
 * Schema field names should match with the POJO variable names<br>
 * Assumption is that each field in the delimited data should map to a simple
 * java type.<br>
 * <br>
 * <b>Properties</b> <br>
 * <b>splitRegexPattern</b>:Regex pattern as a string<br>
 *
 * @displayName RegexSplitter
 * @category Parsers
 * @tags csv pojo parser regex
 * @since 3.6.0
 */
public class RegexSplitter extends CsvParser
{
  @NotNull
  private  String splitRegexPattern;

  private transient Pattern pattern;

  public void setSplitRegexPattern(String splitRegexPattern)
  {
    this.splitRegexPattern = splitRegexPattern;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    pattern = Pattern.compile(splitRegexPattern);
  }

  @Override
  public void processTuple(byte[] tuple)
  {
    if (tuple == null) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(null, "Blank/null tuple"));
      }
      errorTupleCount++;
      return;
    }
    String incomingString = new String(tuple);
    if (StringUtils.isBlank(incomingString) || StringUtils.equals(incomingString, header)) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(incomingString, "Blank/header tuple"));
      }
      errorTupleCount++;
      return;
    }
    try {
      if (out.isConnected() && clazz != null) {
          Matcher matcher = pattern.matcher(incomingString);

          Constructor<?> ctor = clazz.getConstructor();
          Object object = ctor.newInstance();

          while (matcher.find()) {
            for (int i = 1; i <= matcher.groupCount(); i++) {
              if (delimitedParserSchema.getFields().get(i - 1).getType() == DelimitedSchema.FieldType.DATE) {
                DateTimeConverter dtConverter = new DateConverter();
                dtConverter.setPattern((String)delimitedParserSchema.getFields().get(i - 1).getConstraints().get(DelimitedSchema.DATE_FORMAT));
                ConvertUtils.register(dtConverter, Date.class);
              }
              BeanUtils.setProperty(object, delimitedParserSchema.getFields().get(i - 1).getName(), matcher.group(i));
            }
          }
          out.emit(object);
          emittedObjectCount++;
        }

    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | InstantiationException | ConversionException e) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(incomingString, e.getMessage()));
        logger.debug("Regex Expression : {} Incoming tuple : {}",splitRegexPattern, incomingString);
      }
      errorTupleCount++;
      logger.error("Tuple could not be parsed. Incoming Tuple : {} Reason {}", incomingString,e.getMessage());
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(RegexSplitter.class);
}
