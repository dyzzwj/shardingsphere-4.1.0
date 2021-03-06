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

package org.apache.shardingsphere.underlying.executor.engine;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.shardingsphere.underlying.common.exception.ShardingSphereException;
import org.apache.shardingsphere.underlying.executor.engine.impl.ShardingSphereExecutorService;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Executor engine.
 */
public final class ExecutorEngine implements AutoCloseable {
    
    private final ShardingSphereExecutorService executorService;
    
    public ExecutorEngine(final int executorSize) {
        executorService = new ShardingSphereExecutorService(executorSize);
    }
    
    /**
     * Execute.
     *
     * @param inputGroups input groups
     * @param callback grouped callback
     * @param <I> type of input value
     * @param <O> type of return value
     * @return execute result
     * @throws SQLException throw if execute failure
     */
    public <I, O> List<O> execute(final Collection<InputGroup<I>> inputGroups, final GroupedCallback<I, O> callback) throws SQLException {
        return execute(inputGroups, null, callback, false);
    }
    
    /**
     * Execute.
     *
     * @param inputGroups input groups
     * @param firstCallback first grouped callback
     * @param callback other grouped callback
     * @param serial whether using multi thread execute or not
     * @param <I> type of input value
     * @param <O> type of return value
     * @return execute result
     * @throws SQLException throw if execute failure
     */
    public <I, O> List<O> execute(final Collection<InputGroup<I>> inputGroups, 
                                  final GroupedCallback<I, O> firstCallback, final GroupedCallback<I, O> callback, final boolean serial) throws SQLException {
        if (inputGroups.isEmpty()) {
            return Collections.emptyList();
        }
        //???????????? or ????????????
        return serial ? serialExecute(inputGroups, firstCallback, callback) : parallelExecute(inputGroups, firstCallback, callback);
    }

    //????????????
    private <I, O> List<O> serialExecute(final Collection<InputGroup<I>> inputGroups, final GroupedCallback<I, O> firstCallback, final GroupedCallback<I, O> callback) throws SQLException {
        Iterator<InputGroup<I>> inputGroupsIterator = inputGroups.iterator();
        InputGroup<I> firstInputs = inputGroupsIterator.next();
        List<O> result = new LinkedList<>(syncExecute(firstInputs, null == firstCallback ? callback : firstCallback));
        for (InputGroup<I> each : Lists.newArrayList(inputGroupsIterator)) {
            //?????????????????????????????????callback??????
            result.addAll(syncExecute(each, callback));
        }
        return result;
    }

    // ??????????????????????????????????????????????????????????????????????????????????????????(firstInputs)??????????????????????????????????????????(inputGroupsIterator)
    private <I, O> List<O> parallelExecute(final Collection<InputGroup<I>> inputGroups, final GroupedCallback<I, O> firstCallback, final GroupedCallback<I, O> callback) throws SQLException {
        Iterator<InputGroup<I>> inputGroupsIterator = inputGroups.iterator();
        //???????????????sql inputGroupsIterator????????????????????????sql???????????????sql
        InputGroup<I> firstInputs = inputGroupsIterator.next();
        // ??????????????????????????????
        Collection<ListenableFuture<Collection<O>>> restResultFutures = asyncExecute(Lists.newArrayList(inputGroupsIterator), callback);
        //?????????????????????????????????callback??????
        return getGroupResults(syncExecute(firstInputs, null == firstCallback ? callback : firstCallback), restResultFutures);
    }
    
    private <I, O> Collection<O> syncExecute(final InputGroup<I> inputGroup, final GroupedCallback<I, O> callback) throws SQLException {
        return callback.execute(inputGroup.getInputs(), true, ExecutorDataMap.getValue());
    }
    
    private <I, O> Collection<ListenableFuture<Collection<O>>> asyncExecute(final List<InputGroup<I>> inputGroups, final GroupedCallback<I, O> callback) {
        Collection<ListenableFuture<Collection<O>>> result = new LinkedList<>();
        for (InputGroup<I> each : inputGroups) {
            //????????????
            result.add(asyncExecute(each, callback));
        }
        return result;
    }
    
    private <I, O> ListenableFuture<Collection<O>> asyncExecute(final InputGroup<I> inputGroup, final GroupedCallback<I, O> callback) {
        final Map<String, Object> dataMap = ExecutorDataMap.getValue();
        //???????????????
        //SQLExecuteCallback.execute:PreparedStatementExecutor??????executeQuery()??????callback
        return executorService.getExecutorService().submit(() -> callback.execute(inputGroup.getInputs(), false, dataMap));
    }
    
    private <O> List<O> getGroupResults(final Collection<O> firstResults, final Collection<ListenableFuture<Collection<O>>> restFutures) throws SQLException {
        List<O> result = new LinkedList<>(firstResults);
        for (ListenableFuture<Collection<O>> each : restFutures) {
            try {
                //??????
                result.addAll(each.get());
            } catch (final InterruptedException | ExecutionException ex) {
                return throwException(ex);
            }
        }
        return result;
    }
    
    private <O> List<O> throwException(final Exception exception) throws SQLException {
        if (exception.getCause() instanceof SQLException) {
            throw (SQLException) exception.getCause();
        }
        throw new ShardingSphereException(exception);
    }
    
    @Override
    public void close() {
        executorService.close();
    }
}
