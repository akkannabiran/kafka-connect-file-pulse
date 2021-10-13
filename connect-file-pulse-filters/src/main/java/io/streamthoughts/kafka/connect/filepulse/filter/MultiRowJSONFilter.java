/*
 * Copyright 2019-2020 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.streamthoughts.kafka.connect.filepulse.config.JSONFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.json.DefaultJSONStructConverter;
import io.streamthoughts.kafka.connect.filepulse.json.JSONStructConverter;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.List;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Map;

public class MultiRowJSONFilter extends AbstractMergeRecordFilter<MultiRowJSONFilter> {
    private final JSONStructConverter converter = JSONStructConverter.createDefault();

    private JSONFilterConfig configs;
    private ObjectMapper objectMapper = new ObjectMapper();
    private JavaType javaType = TypeFactory.defaultInstance().constructParametricType(List.class, Object.class);
    private Logger logger = LoggerFactory.getLogger(MultiRowJSONFilter.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        configs = new JSONFilterConfig(props);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return JSONFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RecordsIterable<TypedStruct> apply(final FilterContext context, final TypedStruct record) {
        final String value = extractJsonField(checkIsNotNull(record.get(configs.source())));
        final List<TypedStruct> listOfTypedStruct = new LinkedList<>();

        try {
            List<Object> listOfObjects = objectMapper.readValue(value, javaType);
            listOfObjects.forEach(object -> {
                try {
                    final TypedValue typedValue = converter.readJson(objectMapper.writeValueAsString(object));
                    listOfTypedStruct.add(typedValue.getStruct());
                } catch (Exception e) {
                    logger.error("Exception caught while transforming single object and continuing the process {}",
                            object, e);
                }
            });
            return new RecordsIterable<>(listOfTypedStruct);
        } catch (Exception e) {
            throw new FilterException(e.getLocalizedMessage(), e.getCause());
        }
    }

    private String extractJsonField(final TypedValue value) {
        switch (value.type()) {
            case STRING:
                return value.getString();
            case BYTES:
                return new String(value.getBytes(), configs.charset());
            default:
                throw new FilterException(
                        "Invalid field '" + configs.source() + "', cannot parse JSON field of type '" + value.type() + "'"
                );
        }
    }

    private TypedValue checkIsNotNull(final TypedValue value) {
        if (value.isNull()) {
            throw new FilterException(
                    "Invalid field '" + configs.source() + "', cannot convert empty value to JSON");
        }
        return value;
    }

    private String targetField() {
        return configs.target() != null ? configs.target() : configs.source();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Set<String> overwrite() {
        return configs.target() == null && !configs.merge() ?
                Collections.singleton(configs.source())
                : configs.overwrite() ;
    }
}