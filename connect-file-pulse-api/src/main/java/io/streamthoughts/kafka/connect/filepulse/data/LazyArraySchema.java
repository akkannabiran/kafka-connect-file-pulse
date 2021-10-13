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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class LazyArraySchema extends ArraySchema implements Schema {

    private final Collection<?>  list;

    private Schema valueSchema;

    /**
     * Creates a new {@link LazyArraySchema} for the specified type.
     *
     * @param list the {@link List} instance.
     */
    LazyArraySchema(final Collection<?> list) {
        super(null);
        this.list = list;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema valueSchema() {
        if (valueSchema == null) {
            if (list.isEmpty()) {
                throw new DataException("Cannot infer value type because LIST is empty");
            }
            evaluateList(list);
            valueSchema = SchemaSupplier.lazy(list.iterator().next()).get();
        }
        return valueSchema;
    }

    private void evaluateList(Collection<?> list) {
        Iterator<?> iterator = list.iterator();
        Object object = iterator.next();
        if (object instanceof TypedStruct) {
            while (iterator.hasNext()) {
                ((TypedStruct) iterator.next()).schema().fields().forEach(nextTypedField ->
                        ((TypedStruct) object).schema().fields().forEach(firstTypeField -> {
                            if (firstTypeField.name().equals(nextTypedField.name())) {
                                if (firstTypeField.type() == Type.NULL || !firstTypeField.schema().isResolvable()) {
                                    ((TypedStruct) object).schema()
                                            .set(nextTypedField.name(), nextTypedField.schema());
                                }
                                evaluateListOrStruct(firstTypeField, nextTypedField);
                            }
                        }));
            }
        }
    }

    private void evaluateListOrStruct(TypedField firstTypedField, TypedField nextTypedField) {
        if (firstTypedField.schema() instanceof StructSchema && nextTypedField.schema() instanceof StructSchema) {
            ((StructSchema) firstTypedField.schema()).fields().forEach(iFirstTypedField ->
                    ((StructSchema) nextTypedField.schema()).fields().forEach(iNextTypedField -> {
                        if (iFirstTypedField.name().equals(iNextTypedField.name())) {
                            if (iFirstTypedField.type() == Type.NULL || !iFirstTypedField.schema().isResolvable()) {
                                ((StructSchema) firstTypedField.schema())
                                        .set(iFirstTypedField.name(), iNextTypedField.schema());
                            }
                            evaluateListOrStruct(iFirstTypedField, iNextTypedField);
                        }
                    }));
        } else if (firstTypedField.schema() instanceof LazyArraySchema &&
                nextTypedField.schema() instanceof LazyArraySchema) {
            Object lObject = ((LazyArraySchema) firstTypedField.schema()).list.iterator().next();
            Object rObject = ((LazyArraySchema) nextTypedField.schema()).list.iterator().next();
            if (lObject instanceof TypedStruct && rObject instanceof TypedStruct) {
                ((TypedStruct) lObject).schema().fields().forEach(lTypedField ->
                        ((TypedStruct) rObject).schema().fields().forEach(rTypedField -> {
                            if (rTypedField.name().equals(lTypedField.name())) {
                                if (lTypedField.type() == Type.NULL || !lTypedField.schema().isResolvable()) {
                                    ((TypedStruct) lObject).schema()
                                            .set(lTypedField.name(), rTypedField.schema());
                                }
                                evaluateListOrStruct(lTypedField, rTypedField);
                            }
                        }));
                evaluateList(((LazyArraySchema) firstTypedField.schema()).list);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isResolvable() {
        return valueSchema != null || !list.isEmpty();
    }
}