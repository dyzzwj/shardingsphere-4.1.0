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

package org.apache.shardingsphere.shardingjdbc.jdbc.adapter;

import lombok.SneakyThrows;
import org.apache.shardingsphere.shardingjdbc.jdbc.adapter.invocation.JdbcMethodInvocation;

import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Adapter for {@code java.sql.Wrapper}.
 *
 * JDBC Wrapper适配类.
 */
public abstract class WrapperAdapter implements Wrapper {

    /**
     * 记录的方法数组
     */
    private final Collection<JdbcMethodInvocation> jdbcMethodInvocations = new ArrayList<>();
    
    @SuppressWarnings("unchecked")
    @Override
    public final <T> T unwrap(final Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException(String.format("[%s] cannot be unwrapped as [%s]", getClass().getName(), iface.getName()));
    }
    
    @Override
    public final boolean isWrapperFor(final Class<?> iface) {
        return iface.isInstance(this);
    }
    
    /**
     * 记录方法调用.
     *  负责记录外围应用的一些设置方法的调用，例如setAutoCommit、setReadOnly、setFetchSize、setMaxFieldSize等，在外围程序调用ShardingConnection的setAutoCommit、setReadOnly以及ShardingPreparedStatement的setFetchSize、setMaxFieldSize时进行调用
     * @param targetClass target class
     * @param methodName method name
     * @param argumentTypes argument types
     * @param arguments arguments
     */
    @SneakyThrows
    public final void recordMethodInvocation(final Class<?> targetClass, final String methodName, final Class<?>[] argumentTypes, final Object[] arguments) {
        jdbcMethodInvocations.add(new JdbcMethodInvocation(targetClass.getMethod(methodName, argumentTypes), arguments));
    }
    
    /**
     * 回放记录的方法调用.
     * 完成在指定目标对象回放这些方法调用，会在底层真实JDBC类（DB driver、数据库连接池等）时进行重新调用。
     * @param target target object
     */
    public final void replayMethodsInvocation(final Object target) {
        for (JdbcMethodInvocation each : jdbcMethodInvocations) {
            each.invoke(target);
        }
    }
}
