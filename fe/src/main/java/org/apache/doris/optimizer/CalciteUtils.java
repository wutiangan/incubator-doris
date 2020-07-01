// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.optimizer;


import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.doris.catalog.*;
import org.apache.doris.catalog.Table;

import java.util.List;

import static org.apache.doris.catalog.PrimitiveType.*;

public class CalciteUtils {
    public static SchemaPlus registerMockSchema() {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("USERS", new AbstractTable() { //note: add a table
            @Override
            public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();

                builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
                builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.CHAR));
                builder.add("AGE", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
                return builder.build();
            }
        });

        rootSchema.add("JOBS", new AbstractTable() {
            @Override
            public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();

                builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
                builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.CHAR));
                builder.add("company", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.CHAR));
                return builder.build();
            }
        });
        return rootSchema;
    }
    public static SchemaPlus registerRootSchema(String db) {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        Database database = Catalog.getCurrentCatalog().getDb(db);

        for (Table table : database.getTables()) {
            SchemaPlus schema = rootSchema.add(table.getName(), new AbstractSchema());
            TableImpl tableDef = new TableImpl(table);
            rootSchema.add(table.getName(), tableDef);
        }
        return rootSchema;
    }

    public static SqlConformance conformance(FrameworkConfig config) {
        final Context context = config.getContext();
        if (context != null) {
            final CalciteConnectionConfig connectionConfig =
                    context.unwrap(CalciteConnectionConfig.class);
            if (connectionConfig != null) {
                return connectionConfig.conformance();
            }
        }
        return SqlConformanceEnum.DEFAULT;
    }

    public static RexBuilder createRexBuilder(RelDataTypeFactory typeFactory) {
        return new RexBuilder(typeFactory);
    }

    public static class ViewExpanderImpl implements RelOptTable.ViewExpander {
        public ViewExpanderImpl() {
        }

        @Override
        public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath,
                                  List<String> viewPath) {
            return null;
        }
    }

    private static final class TableImpl extends AbstractTable {

        final Table table;

        private TableImpl(Table table) {
            this.table = table;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
            for (Column column : this.table.getBaseSchema()) {
                builder.add(column.getName(), convertType(column.getType(), typeFactory, column.isAllowNull()));
            }
            return builder.build();
        }

        private static RelDataType convertType(
                Type type,
                RelDataTypeFactory typeFactory, boolean nullable
        ) {
            RelDataType relDataType = convertTypeNotNull(type, typeFactory);
            if (nullable) {
                return typeFactory.createTypeWithNullability(relDataType, true);
            } else {
                return relDataType;
            }
        }

        private static RelDataType convertTypeNotNull(
                Type type,
                RelDataTypeFactory typeFactory
        ) {

            switch (type.getPrimitiveType()) {
                case TINYINT:
                    return typeFactory.createSqlType(SqlTypeName.TINYINT);
                case SMALLINT:
                    return typeFactory.createSqlType(SqlTypeName.SMALLINT);
                case INT:
                    return typeFactory.createSqlType(SqlTypeName.INTEGER);
                case BIGINT:
                    return typeFactory.createSqlType(SqlTypeName.BIGINT);
                case VARCHAR:
                    return typeFactory.createSqlType(SqlTypeName.CHAR);
                case CHAR:
                    return typeFactory.createSqlType(SqlTypeName.CHAR);
                case DATE:
                    return typeFactory.createSqlType(SqlTypeName.DATE);
                case DATETIME:
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                default:
                    return typeFactory.createSqlType(SqlTypeName.ANY);

            }
        }
    }
}
