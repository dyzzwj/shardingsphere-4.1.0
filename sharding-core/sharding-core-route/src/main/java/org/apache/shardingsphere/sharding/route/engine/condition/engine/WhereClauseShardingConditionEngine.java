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

package org.apache.shardingsphere.sharding.route.engine.condition.engine;

import com.google.common.collect.Range;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.strategy.route.value.ListRouteValue;
import org.apache.shardingsphere.core.strategy.route.value.RangeRouteValue;
import org.apache.shardingsphere.core.strategy.route.value.RouteValue;
import org.apache.shardingsphere.sharding.route.engine.condition.AlwaysFalseRouteValue;
import org.apache.shardingsphere.sharding.route.engine.condition.AlwaysFalseShardingCondition;
import org.apache.shardingsphere.sharding.route.engine.condition.Column;
import org.apache.shardingsphere.sharding.route.engine.condition.ShardingCondition;
import org.apache.shardingsphere.sharding.route.engine.condition.generator.ConditionValueGeneratorFactory;
import org.apache.shardingsphere.sql.parser.binder.metadata.schema.SchemaMetaData;
import org.apache.shardingsphere.sql.parser.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.binder.type.WhereAvailable;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.predicate.AndPredicate;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.predicate.PredicateSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.underlying.common.exception.ShardingSphereException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Sharding condition engine for where clause.
 */
@RequiredArgsConstructor
public final class WhereClauseShardingConditionEngine {
    
    private final ShardingRule shardingRule;
    
    private final SchemaMetaData schemaMetaData;
    
    /**
     * Create sharding conditions.
     * 
     * @param sqlStatementContext SQL statement context
     * @param parameters SQL parameters
     * @return sharding conditions
     */
    public List<ShardingCondition> createShardingConditions(final SQLStatementContext sqlStatementContext, final List<Object> parameters) {
        if (!(sqlStatementContext instanceof WhereAvailable)) {
            return Collections.emptyList();
        }
        List<ShardingCondition> result = new ArrayList<>();
        Optional<WhereSegment> whereSegment = ((WhereAvailable) sqlStatementContext).getWhere();
        if (whereSegment.isPresent()) {
            result.addAll(createShardingConditions(sqlStatementContext, whereSegment.get().getAndPredicates(), parameters));
        }
        // FIXME process subquery
//        Collection<SubqueryPredicateSegment> subqueryPredicateSegments = sqlStatementContext.findSQLSegments(SubqueryPredicateSegment.class);
//        for (SubqueryPredicateSegment each : subqueryPredicateSegments) {
//            Collection<ShardingCondition> subqueryShardingConditions = createShardingConditions((WhereSegmentAvailable) sqlStatementContext, each.getAndPredicates(), parameters);
//            if (!result.containsAll(subqueryShardingConditions)) {
//                result.addAll(subqueryShardingConditions);
//            }
//        }
        return result;
    }
    
    private Collection<ShardingCondition> createShardingConditions(final SQLStatementContext sqlStatementContext, final Collection<AndPredicate> andPredicates, final List<Object> parameters) {
        Collection<ShardingCondition> result = new LinkedList<>();
        for (AndPredicate each : andPredicates) {
            // ??????where????????????????????????????????????????????????????????????????????????
            Map<Column, Collection<RouteValue>> routeValueMap = createRouteValueMap(sqlStatementContext, each, parameters);
            if (routeValueMap.isEmpty()) {
                return Collections.emptyList();
            }
            // ?????????????????????map????????????????????????????????????????????????????????????
            result.add(createShardingCondition(routeValueMap));
        }
        return result;
    }
    
    private Map<Column, Collection<RouteValue>> createRouteValueMap(final SQLStatementContext sqlStatementContext, final AndPredicate andPredicate, final List<Object> parameters) {
        Map<Column, Collection<RouteValue>> result = new HashMap<>();
        for (PredicateSegment each : andPredicate.getPredicates()) {
            Optional<String> tableName = sqlStatementContext.getTablesContext().findTableName(each.getColumn(), schemaMetaData);
            if (!tableName.isPresent() || !shardingRule.isShardingColumn(each.getColumn().getIdentifier().getValue(), tableName.get())) {
                continue;
            }
            Column column = new Column(each.getColumn().getIdentifier().getValue(), tableName.get());
            /**
             * ??????????????????????????????????????????=???in???ListRouteValue?????????>???<???between???????????????RangeRouteValue??????
             */
            Optional<RouteValue> routeValue = ConditionValueGeneratorFactory.generate(each.getRightValue(), column, parameters);
            if (!routeValue.isPresent()) {
                continue;
            }
            if (!result.containsKey(column)) {
                result.put(column, new LinkedList<>());
            }
            result.get(column).add(routeValue.get());
        }
        return result;
    }
    
    private ShardingCondition createShardingCondition(final Map<Column, Collection<RouteValue>> routeValueMap) {
        ShardingCondition result = new ShardingCondition();
        for (Entry<Column, Collection<RouteValue>> entry : routeValueMap.entrySet()) {
            try {
                RouteValue routeValue = mergeRouteValues(entry.getKey(), entry.getValue());
                if (routeValue instanceof AlwaysFalseRouteValue) {
                    return new AlwaysFalseShardingCondition();
                }
                result.getRouteValues().add(routeValue);
            } catch (final ClassCastException ex) {
                throw new ShardingSphereException("Found different types for sharding value `%s`.", entry.getKey());
            }
        }
        return result;
    }
    
    @SuppressWarnings("unchecked")
    private RouteValue mergeRouteValues(final Column column, final Collection<RouteValue> routeValues) {
        Collection<Comparable<?>> listValue = null;
        Range<Comparable<?>> rangeValue = null;
        for (RouteValue each : routeValues) {
            if (each instanceof ListRouteValue) {
                listValue = mergeListRouteValues(((ListRouteValue) each).getValues(), listValue);
                if (listValue.isEmpty()) {
                    return new AlwaysFalseRouteValue();
                }
            } else if (each instanceof RangeRouteValue) {
                try {
                    rangeValue = mergeRangeRouteValues(((RangeRouteValue) each).getValueRange(), rangeValue);
                } catch (final IllegalArgumentException ex) {
                    return new AlwaysFalseRouteValue();
                }
            }
        }
        if (null == listValue) {
            return new RangeRouteValue<>(column.getName(), column.getTableName(), rangeValue);
        }
        if (null == rangeValue) {
            return new ListRouteValue<>(column.getName(), column.getTableName(), listValue);
        }
        listValue = mergeListAndRangeRouteValues(listValue, rangeValue);
        return listValue.isEmpty() ? new AlwaysFalseRouteValue() : new ListRouteValue<>(column.getName(), column.getTableName(), listValue);
    }
    
    private Collection<Comparable<?>> mergeListRouteValues(final Collection<Comparable<?>> value1, final Collection<Comparable<?>> value2) {
        if (null == value2) {
            return value1;
        }
        value1.retainAll(value2);
        return value1;
    }
    
    private Range<Comparable<?>> mergeRangeRouteValues(final Range<Comparable<?>> value1, final Range<Comparable<?>> value2) {
        return null == value2 ? value1 : value1.intersection(value2);
    }
    
    private Collection<Comparable<?>> mergeListAndRangeRouteValues(final Collection<Comparable<?>> listValue, final Range<Comparable<?>> rangeValue) {
        Collection<Comparable<?>> result = new LinkedList<>();
        for (Comparable<?> each : listValue) {
            if (rangeValue.contains(each)) {
                result.add(each);
            }
        }
        return result;
    }
}
