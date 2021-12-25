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

package org.apache.shardingsphere.sql.parser;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.shardingsphere.sql.parser.cache.SQLParseResultCache;
import org.apache.shardingsphere.sql.parser.core.parser.SQLParserExecutor;
import org.apache.shardingsphere.sql.parser.core.visitor.ParseTreeVisitorFactory;
import org.apache.shardingsphere.sql.parser.hook.ParsingHook;
import org.apache.shardingsphere.sql.parser.hook.SPIParsingHook;
import org.apache.shardingsphere.sql.parser.core.visitor.VisitorRule;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;

import java.util.Optional;

/**
 * SQL parser engine.
 */
@RequiredArgsConstructor
public final class SQLParserEngine {
    
    private final String databaseTypeName;
    
    private final SQLParseResultCache cache = new SQLParseResultCache();
    
    // TODO check skywalking plugin
    /*
     * To make sure SkyWalking will be available at the next release of ShardingSphere,
     * a new plugin should be provided to SkyWalking project if this API changed.
     *
     * @see <a href="https://github.com/apache/skywalking/blob/master/docs/en/guides/Java-Plugin-Development-Guide.md#user-content-plugin-development-guide">Plugin Development Guide</a>
     *
     */
    /**
     * Parse SQL.
     *
     * @param sql SQL
     * @param useCache use cache or not
     * @return SQL statement
     */
    public SQLStatement parse(final String sql, final boolean useCache) {
        //钩子函数回调
        ParsingHook parsingHook = new SPIParsingHook();
        parsingHook.start(sql);
        try {
            //解析
            SQLStatement result = parse0(sql, useCache);
            //钩子函数回调
            parsingHook.finishSuccess(result);
            return result;
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            parsingHook.finishFailure(ex);
            throw ex;
        }
    }
    
    private SQLStatement parse0(final String sql, final boolean useCache) {
        // 如果缓存中有该SQL的解析结果，则直接复用
        if (useCache) {
            Optional<SQLStatement> cachedSQLStatement = cache.getSQLStatement(sql);
            if (cachedSQLStatement.isPresent()) {
                return cachedSQLStatement.get();
            }
        }
        // 解析SQL生成AST，ParseTree是antlr对应的解析树接口
        ParseTree parseTree = new SQLParserExecutor(databaseTypeName, sql).execute().getRootNode();
        //通过ParseTreeVisitor访问解析树
        SQLStatement result = (SQLStatement) ParseTreeVisitorFactory.newInstance(databaseTypeName, VisitorRule.valueOf(parseTree.getClass())).visit(parseTree);
        if (useCache) {
            cache.put(sql, result);
        }
        return result;
    }
}
