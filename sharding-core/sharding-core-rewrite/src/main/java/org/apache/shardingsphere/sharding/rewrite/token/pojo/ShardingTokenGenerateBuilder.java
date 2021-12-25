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

package org.apache.shardingsphere.sharding.rewrite.token.pojo;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.rule.aware.ShardingRuleAware;
import org.apache.shardingsphere.underlying.rewrite.sql.token.generator.aware.RouteContextAware;
import org.apache.shardingsphere.sharding.rewrite.token.generator.IgnoreForSingleRoute;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.AggregationDistinctTokenGenerator;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.DistinctProjectionPrefixTokenGenerator;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.IndexTokenGenerator;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.OffsetTokenGenerator;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.OrderByTokenGenerator;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.ProjectionsTokenGenerator;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.RowCountTokenGenerator;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.ShardingInsertValuesTokenGenerator;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.TableTokenGenerator;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.keygen.GeneratedKeyAssignmentTokenGenerator;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.keygen.GeneratedKeyForUseDefaultInsertColumnsTokenGenerator;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.keygen.GeneratedKeyInsertColumnTokenGenerator;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.keygen.GeneratedKeyInsertValuesTokenGenerator;
import org.apache.shardingsphere.underlying.rewrite.sql.token.generator.SQLTokenGenerator;
import org.apache.shardingsphere.underlying.rewrite.sql.token.generator.builder.SQLTokenGeneratorBuilder;
import org.apache.shardingsphere.underlying.route.context.RouteContext;

import java.util.Collection;
import java.util.LinkedList;

/**
 * SQL token generator builder for sharding.
 */
@RequiredArgsConstructor
public final class ShardingTokenGenerateBuilder implements SQLTokenGeneratorBuilder {
    
    private final ShardingRule shardingRule;
    
    private final RouteContext routeContext;
    
    @Override
    public Collection<SQLTokenGenerator> getSQLTokenGenerators() {
        //构造基本的生成器
        Collection<SQLTokenGenerator> result = buildSQLTokenGenerators();
        for (SQLTokenGenerator each : result) {
            if (each instanceof ShardingRuleAware) {
                ((ShardingRuleAware) each).setShardingRule(shardingRule);
            }
            if (each instanceof RouteContextAware) {
                ((RouteContextAware) each).setRouteContext(routeContext);
            }
        }
        return result;
    }
    
    private Collection<SQLTokenGenerator> buildSQLTokenGenerators() {
        Collection<SQLTokenGenerator> result = new LinkedList<>();
        addSQLTokenGenerator(result, new TableTokenGenerator());//表名token处理，用于真实表名替换
        addSQLTokenGenerator(result, new DistinctProjectionPrefixTokenGenerator());// select distinct关键字处理
        addSQLTokenGenerator(result, new ProjectionsTokenGenerator());// select列名处理，主要是衍生列avg处理
        addSQLTokenGenerator(result, new OrderByTokenGenerator());// Order by Token处理
        addSQLTokenGenerator(result, new AggregationDistinctTokenGenerator());// 聚合函数的distinct关键字处理
        addSQLTokenGenerator(result, new IndexTokenGenerator());// 索引重命名
        addSQLTokenGenerator(result, new OffsetTokenGenerator());// offset 重写
        addSQLTokenGenerator(result, new RowCountTokenGenerator());// rowCount重写
        addSQLTokenGenerator(result, new GeneratedKeyInsertColumnTokenGenerator());// 分布式主键列添加，在insert sql列最后添加
        addSQLTokenGenerator(result, new GeneratedKeyForUseDefaultInsertColumnsTokenGenerator());// insert SQL使用默认列名时需要完成补齐真实列名，包括自增列
        addSQLTokenGenerator(result, new GeneratedKeyAssignmentTokenGenerator());// SET自增键生成
        addSQLTokenGenerator(result, new ShardingInsertValuesTokenGenerator());// insert SQL 的values Token解析，为后续添加自增值做准备
        addSQLTokenGenerator(result, new GeneratedKeyInsertValuesTokenGenerator());//为insert values添加自增列值
        return result;
    }
    
    private void addSQLTokenGenerator(final Collection<SQLTokenGenerator> sqlTokenGenerators, final SQLTokenGenerator toBeAddedSQLTokenGenerator) {
        if (toBeAddedSQLTokenGenerator instanceof IgnoreForSingleRoute && routeContext.getRouteResult().isSingleRouting()) {
            return;
        }
        sqlTokenGenerators.add(toBeAddedSQLTokenGenerator);
    }
}
