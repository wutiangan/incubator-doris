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

import com.google.common.base.Preconditions;
//import com.sun.deploy.security.ruleset.ExceptionRule;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
//import org.apache.calcite.adapter.enumerable.EnumerableIntersectRule;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.doris.analysis.Analyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class CalcitePlanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlVolcanoTest.class);

    public RelNode plan(String sql, SchemaPlus rootSchema) {
        final FrameworkConfig fromworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(rootSchema)
                .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
                .build();

        // use HepPlanner
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
        // add rules
        planner.addRule(FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN);
        planner.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);
        planner.addRule(PruneEmptyRules.PROJECT_INSTANCE);
        // add ConverterRule
        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_INTERSECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_UNION_RULE);

        try {
            SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            // sql parser
            SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
            SqlNode parsed = parser.parseStmt();
            LOGGER.info("The SqlNode after parsed is:\n{}", parsed.toString());

            CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
                    CalciteSchema.from(rootSchema),
                    CalciteSchema.from(rootSchema).path(null),
                    factory,
                    new CalciteConnectionConfigImpl(new Properties()));

            // sql validate
            SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), calciteCatalogReader,
                    factory, CalciteUtils.conformance(fromworkConfig));
            SqlNode validated = validator.validate(parsed);
            LOGGER.info("The SqlNode after validated is:\n{}", validated.toString());

            final RexBuilder rexBuilder = CalciteUtils.createRexBuilder(factory);
            final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

            // init SqlToRelConverter config
            final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
                    .withConfig(fromworkConfig.getSqlToRelConverterConfig())
                    .withTrimUnusedFields(false)
                    .withConvertTableAccess(false)
                    .build();
            // SqlNode toRelNode
            final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(new CalciteUtils.ViewExpanderImpl(),
                    validator, calciteCatalogReader, cluster, fromworkConfig.getConvertletTable(), config);
            RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);

            root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
            final RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
            root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
            RelNode relNode = root.rel;
            LOGGER.info("The relational expression string before optimized is:\n{}", RelOptUtil.toString(relNode));

            RelTraitSet desiredTraits =
                    relNode.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
            relNode = planner.changeTraits(relNode, desiredTraits);

            planner.setRoot(relNode);
            relNode = planner.findBestExp();
            System.out.println("-----------------------------------------------------------");
            System.out.println("The Best relational expression string:");
            System.out.println(RelOptUtil.toString(relNode));

            System.out.println("relNode_to_string:" + relNode.toString());
            System.out.println("relNode_to_string:" + relNode.getDescription());
            System.out.println("-----------------------------------------------------------");
            return relNode;
        } catch (Exception e) {
            e.printStackTrace();
            Preconditions.checkState(false);
            return null;
        }
    }
}