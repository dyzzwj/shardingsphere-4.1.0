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

package org.apache.shardingsphere.masterslave.route.engine;

import org.apache.shardingsphere.core.rule.MasterSlaveRule;
import org.apache.shardingsphere.masterslave.route.engine.impl.MasterSlaveDataSourceRouter;
import org.apache.shardingsphere.underlying.common.config.properties.ConfigurationProperties;
import org.apache.shardingsphere.underlying.common.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.underlying.route.context.RouteContext;
import org.apache.shardingsphere.underlying.route.context.RouteMapper;
import org.apache.shardingsphere.underlying.route.context.RouteResult;
import org.apache.shardingsphere.underlying.route.context.RouteUnit;
import org.apache.shardingsphere.underlying.route.decorator.RouteDecorator;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

/**
 * Route decorator for master-slave.
 */
public final class MasterSlaveRouteDecorator implements RouteDecorator<MasterSlaveRule> {
    
    @Override
    public RouteContext decorate(final RouteContext routeContext, final ShardingSphereMetaData metaData, final MasterSlaveRule masterSlaveRule, final ConfigurationProperties properties) {
        if (routeContext.getRouteResult().getRouteUnits().isEmpty()) {
            // 获取路由的数据源名称
            String dataSourceName = new MasterSlaveDataSourceRouter(masterSlaveRule).route(routeContext.getSqlStatementContext().getSqlStatement());
            RouteResult routeResult = new RouteResult();
            routeResult.getRouteUnits().add(new RouteUnit(new RouteMapper(dataSourceName, dataSourceName), Collections.emptyList()));
            return new RouteContext(routeContext.getSqlStatementContext(), Collections.emptyList(), routeResult);
        }
        // 分库分表+读写分离模式下，在计算完数据分片库路由（数据源实际是主从规则名称）后，
        // 还需要根据主从配置，替换为真实的数据源，因此需要先进行删除，再添加真实数据源
        Collection<RouteUnit> toBeRemoved = new LinkedList<>();
        Collection<RouteUnit> toBeAdded = new LinkedList<>();
        for (RouteUnit each : routeContext.getRouteResult().getRouteUnits()) {
            if (masterSlaveRule.getName().equalsIgnoreCase(each.getDataSourceMapper().getActualName())) {
                toBeRemoved.add(each);
                String actualDataSourceName = new MasterSlaveDataSourceRouter(masterSlaveRule).route(routeContext.getSqlStatementContext().getSqlStatement());
                toBeAdded.add(new RouteUnit(new RouteMapper(each.getDataSourceMapper().getLogicName(), actualDataSourceName), each.getTableMappers()));
            }
        }
        routeContext.getRouteResult().getRouteUnits().removeAll(toBeRemoved);
        routeContext.getRouteResult().getRouteUnits().addAll(toBeAdded);
        return routeContext;
    }
    
    @Override
    public int getOrder() {
        return 10;
    }
    
    @Override
    public Class<MasterSlaveRule> getType() {
        return MasterSlaveRule.class;
    }
}
