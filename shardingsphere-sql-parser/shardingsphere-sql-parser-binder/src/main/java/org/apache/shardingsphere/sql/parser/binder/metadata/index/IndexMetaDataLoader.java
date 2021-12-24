/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.sql.parser.binder.metadata.index;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.sql.parser.binder.metadata.util.JdbcUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;

/**
 * Index meta data loader.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class IndexMetaDataLoader {
    
    private static final String INDEX_NAME = "INDEX_NAME";
    
    /**
     * Load column meta data list.
     * 
     * @param connection connection
     * @param table table name
     * @return index meta data list
     * @throws SQLException SQL exception
     */
    public static Collection<IndexMetaData> load(final Connection connection, final String table) throws SQLException {
        Collection<IndexMetaData> result = new HashSet<>();
        try (ResultSet resultSet = connection.getMetaData().getIndexInfo(connection.getCatalog(), JdbcUtil.getSchema(connection), table, false, false)) {
            while (resultSet.next()) {
                String indexName = resultSet.getString(INDEX_NAME);
                result.add(new IndexMetaData(indexName));
            }
        }
        return result;
    }
}
