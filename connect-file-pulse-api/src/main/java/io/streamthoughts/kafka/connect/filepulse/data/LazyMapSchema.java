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
package io.streamthoughts.kafka.connect.filepulse.data;

import java.util.Map;

public class LazyMapSchema extends MapSchema implements Schema {

    private final Map map;

    private Schema valueSchema;

    /**
     * Creates a new LazyMapSchema for the specified type.
     */
    LazyMapSchema(final Map map) {
        super(null);
        this.map = map;

    }

    @Override
    public Schema valueSchema() {
        if (valueSchema == null) {
            if (map.isEmpty()) {
                throw new DataException("Cannot infer value type because MAP is empty");
            }
            final Object peek = map.values().iterator().next();
            valueSchema = SchemaSupplier.lazy(peek).get();
        }
        return valueSchema;
    }
}