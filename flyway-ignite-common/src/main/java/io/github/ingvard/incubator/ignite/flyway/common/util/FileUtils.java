/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.ingvard.incubator.ignite.flyway.common.util;

import java.io.File;
import java.util.OptionalInt;
import java.util.stream.Stream;

/**
 * File utils.
 */
public class FileUtils {
    /**
     * Returns same file when the file doesn't exist or a file with incremented index in name.
     * <p></p>
     * Example:
     * snapshot.sql, snapshot1.sql, snapshot2.sql and etc.
     *
     * @param firstFileInIdx First file in index.
     */
    public static File fileWithNextIndex(File firstFileInIdx, String format) {
        if (!firstFileInIdx.exists()) {
            return firstFileInIdx;
        }

        String fileName = firstFileInIdx.getName().substring(0, firstFileInIdx.getName().length() - format.length());
        File parentDir = firstFileInIdx.getParentFile();

        OptionalInt lastFileIdxOpt = Stream.of(parentDir.listFiles((ignore, fn) -> fn.startsWith(fileName)))
                .map(File::getName)
                .map(fn -> fn.substring(fileName.length()))
                .map(fn -> fn.substring(0, fn.length() - format.length()))
                .filter(NumericalUtils::isInt)
                .mapToInt(Integer::parseInt)
                .max();

        int newFileIdx = lastFileIdxOpt.orElse(0) + 1;

        return parentDir.toPath().resolve(fileName + newFileIdx + format).toFile();
    }
}
