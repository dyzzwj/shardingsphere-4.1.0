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

package org.apache.shardingsphere.encrypt.rewrite.context;

import org.apache.shardingsphere.encrypt.rewrite.parameter.EncryptParameterRewriterBuilder;
import org.apache.shardingsphere.encrypt.rewrite.token.EncryptTokenGenerateBuilder;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.underlying.common.config.properties.ConfigurationProperties;
import org.apache.shardingsphere.underlying.common.config.properties.ConfigurationPropertyKey;
import org.apache.shardingsphere.underlying.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.underlying.rewrite.context.SQLRewriteContextDecorator;
import org.apache.shardingsphere.underlying.rewrite.parameter.rewriter.ParameterRewriter;

/**
 * SQL rewrite context decorator for encrypt.
 *  加密sql重写装饰器
 *
 */
public final class EncryptSQLRewriteContextDecorator implements SQLRewriteContextDecorator<EncryptRule> {
    
    @SuppressWarnings("unchecked")
    @Override
    public void decorate(final EncryptRule encryptRule, final ConfigurationProperties properties, final SQLRewriteContext sqlRewriteContext) {
        boolean isQueryWithCipherColumn = properties.<Boolean>getValue(ConfigurationPropertyKey.QUERY_WITH_CIPHER_COLUMN);
        for (ParameterRewriter each : new EncryptParameterRewriterBuilder(encryptRule, isQueryWithCipherColumn).getParameterRewriters(sqlRewriteContext.getSchemaMetaData())) {
            if (!sqlRewriteContext.getParameters().isEmpty() && each.isNeedRewrite(sqlRewriteContext.getSqlStatementContext())) {
                each.rewrite(sqlRewriteContext.getParameterBuilder(), sqlRewriteContext.getSqlStatementContext(), sqlRewriteContext.getParameters());
            }
        }
        sqlRewriteContext.addSQLTokenGenerators(new EncryptTokenGenerateBuilder(encryptRule, isQueryWithCipherColumn).getSQLTokenGenerators());
    }
    
    @Override
    public int getOrder() {
        return 20;
    }
    
    @Override
    public Class<EncryptRule> getType() {
        return EncryptRule.class;
    }
}
