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

package org.apache.shardingsphere.sharding.merge.dql.pagination;

import org.apache.shardingsphere.underlying.merge.result.MergedResult;
import org.apache.shardingsphere.underlying.merge.result.impl.decorator.DecoratorMergedResult;
import org.apache.shardingsphere.sql.parser.binder.segment.select.pagination.PaginationContext;

import java.sql.SQLException;

/**
 * Decorator merged result for limit pagination.
 */
public final class LimitDecoratorMergedResult extends DecoratorMergedResult {
    
    private final PaginationContext pagination;
    
    private final boolean skipAll;
    
    private int rowNumber;
    
    public LimitDecoratorMergedResult(final MergedResult mergedResult, final PaginationContext pagination) throws SQLException {
        super(mergedResult);
        this.pagination = pagination;
        skipAll = skipOffset();
    }
    
    private boolean skipOffset() throws SQLException {
        for (int i = 0; i < pagination.getActualOffset(); i++) {
            //如果结果集总概述小于offset值，设置skipAll为true，表示跳过所有结果集，后续next()直接返回false
            if (!getMergedResult().next()) {
                return true;
            }
        }
        rowNumber = 0;
        return false;
    }
    
    @Override
    public boolean next() throws SQLException {
        if (skipAll) {
            return false;
        }
        if (!pagination.getActualRowCount().isPresent()) {
            return getMergedResult().next();
        }
        //由于改写了offset，多个数据节点返回的结果集总数大于SQL中指定的RowCount，因此在next()操作时要记录当前已返回的记录总数rowNumber，同时要判断该值不能大于SQL指定的RowCount
        return ++rowNumber <= pagination.getActualRowCount().get() && getMergedResult().next();
    }
}
