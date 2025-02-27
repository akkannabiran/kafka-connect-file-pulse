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
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.source.SourceOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileContext;

import java.util.Iterator;

/**
 * Default interface to iterate over an input file.
 *
 * @param <T> type of value.
 */
public interface FileInputIterator<T> extends Iterator<RecordsIterable<T>> {

    /**
     * Gets the iterator context.
     * @return  a {@link FileContext} instance.
     */
    FileContext context();

    /**
     * Seeks iterator to the specified startPosition.
     * @param offset  the position from which to seek the iterator.
     */
    void seekTo(final SourceOffset offset);

    /**
     * Reads the next records from the iterator file.
     */
    RecordsIterable<T> next();

    /**
     * Checks whether the iterator file has more records to read.
     * @return {@code true} if the iteration has more elements
     */
    boolean hasNext();

    /**
     * Close the iterator file/input stream.
     */
    void close();

    /**
     * Checks whether this iterator is already close.
     * @return {@code true} if this iterator is close.
     */
    boolean isClose();
}
