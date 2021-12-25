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

package org.apache.shardingsphere.sharding.rewrite.context;

import lombok.Setter;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.sharding.rewrite.parameter.ShardingParameterRewriterBuilder;
import org.apache.shardingsphere.sharding.rewrite.token.pojo.ShardingTokenGenerateBuilder;
import org.apache.shardingsphere.underlying.common.config.properties.ConfigurationProperties;
import org.apache.shardingsphere.underlying.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.underlying.rewrite.context.SQLRewriteContextDecorator;
import org.apache.shardingsphere.underlying.rewrite.parameter.rewriter.ParameterRewriter;
import org.apache.shardingsphere.underlying.rewrite.sql.token.generator.aware.RouteContextAware;
import org.apache.shardingsphere.underlying.route.context.RouteContext;

/**
 * SQL rewrite context decorator for sharding.
 *
 *  数据分片sql重写装饰器
 */
@Setter
public final class ShardingSQLRewriteContextDecorator implements SQLRewriteContextDecorator<ShardingRule>, RouteContextAware {
    
    private RouteContext routeContext;
    
    @SuppressWarnings("unchecked")
    @Override
    public void decorate(final ShardingRule shardingRule, final ConfigurationProperties properties, final SQLRewriteContext sqlRewriteContext) {
        /**
         *  获取参数重写器（参数化SQL才需要），然后依次对SQL重写上下文中的参数构造器parameterBuilder进行重写操作，分片功能下主要是自增键以及分页参数
         *  ShardingGeneratedKeyInsertValueParameterRewriter - insert分布式自增参数重写
         *  ShardingPaginationParameterRewriter - 分页
         */
        for (ParameterRewriter each : new ShardingParameterRewriterBuilder(shardingRule, routeContext).getParameterRewriters(sqlRewriteContext.getSchemaMetaData())) {
            if (!sqlRewriteContext.getParameters().isEmpty() && each.isNeedRewrite(sqlRewriteContext.getSqlStatementContext())) {
                each.rewrite(sqlRewriteContext.getParameterBuilder(), sqlRewriteContext.getSqlStatementContext(), sqlRewriteContext.getParameters());
            }
        }
        //添加分片功能下对应的Token生成器
        sqlRewriteContext.addSQLTokenGenerators(new ShardingTokenGenerateBuilder(shardingRule, routeContext).getSQLTokenGenerators());
    }
    
    @Override
    public int getOrder() {
        return 0;
    }
    
    @Override
    public Class<ShardingRule> getType() {
        return ShardingRule.class;
    }
}
