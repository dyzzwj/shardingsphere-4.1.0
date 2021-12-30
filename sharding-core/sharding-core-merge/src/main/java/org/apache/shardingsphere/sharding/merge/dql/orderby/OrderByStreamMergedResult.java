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

package org.apache.shardingsphere.sharding.merge.dql.orderby;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.shardingsphere.sql.parser.binder.metadata.schema.SchemaMetaData;
import org.apache.shardingsphere.sql.parser.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.underlying.executor.QueryResult;
import org.apache.shardingsphere.underlying.merge.result.impl.stream.StreamMergedResult;
import org.apache.shardingsphere.sql.parser.binder.segment.select.orderby.OrderByItem;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Stream merged result for order by.
 *  使用优先队列实现了基于流模式的排序，由于每个数据集已经有序，所以在next()操作时弹出队列头部元素，然后再取该数据集下一个压入队列，当进行读取数据时直接读取队头元素对应值即可。
 *
 *  这里需要注意的是该优先队列采用的是小顶堆，但实际排序的时候可能是升序也可能是降序，那这里如何保证在不同排序方向时都能正确返回结果呢。玄机在排序元素OrderByValue类，其实现了Comparable接口，其会根据排序方向保证元素值的正确顺序。
 *
 *
 */
public class OrderByStreamMergedResult extends StreamMergedResult {
    
    private final Collection<OrderByItem> orderByItems;
    
    @Getter(AccessLevel.PROTECTED)
    private final Queue<OrderByValue> orderByValuesQueue;
    
    @Getter(AccessLevel.PROTECTED)
    private boolean isFirstNext;
    
    public OrderByStreamMergedResult(final List<QueryResult> queryResults, final SelectStatementContext selectStatementContext, final SchemaMetaData schemaMetaData) throws SQLException {
        this.orderByItems = selectStatementContext.getOrderByContext().getItems();
        this.orderByValuesQueue = new PriorityQueue<>(queryResults.size());
        orderResultSetsToQueue(queryResults, selectStatementContext, schemaMetaData);
        isFirstNext = true;
    }

    //将每个数据节点的查询结果各取一个放入优先队列中，设置当前查询结果为队列头元素，由于优先队列能保证其中元素的顺序性，因此 每次取出的队头元素即为排序最小的。
    private void orderResultSetsToQueue(final List<QueryResult> queryResults, final SelectStatementContext selectStatementContext, final SchemaMetaData schemaMetaData) throws SQLException {
        for (QueryResult each : queryResults) {
            OrderByValue orderByValue = new OrderByValue(each, orderByItems, selectStatementContext, schemaMetaData);
            if (orderByValue.next()) {
                orderByValuesQueue.offer(orderByValue);
            }
        }
        setCurrentQueryResult(orderByValuesQueue.isEmpty() ? queryResults.get(0) : orderByValuesQueue.peek().getQueryResult());
    }
    
    @Override
    public boolean next() throws SQLException {
        if (orderByValuesQueue.isEmpty()) {
            return false;
        }
        if (isFirstNext) {
            isFirstNext = false;
            return true;
        }
        //获取队列头元素，然后取该元素所在数据组的下一个元素，如果存在则继续压入有限队列
        OrderByValue firstOrderByValue = orderByValuesQueue.poll();
        if (firstOrderByValue.next()) {
            orderByValuesQueue.offer(firstOrderByValue);
        }
        if (orderByValuesQueue.isEmpty()) {
            return false;
        }
        setCurrentQueryResult(orderByValuesQueue.peek().getQueryResult());
        return true;
    }
}
