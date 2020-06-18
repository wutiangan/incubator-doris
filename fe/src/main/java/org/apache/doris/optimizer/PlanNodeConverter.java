package org.apache.doris.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import org.apache.doris.analysis.*;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class PlanNodeConverter {
    private static final Logger LOG = LoggerFactory.getLogger(PlanNodeConverter.class);

    private final RelNode          root;
    private RelNode          from;
    private Filter           where;
    private Aggregate        groupBy;
    private Filter           having;
    private RelNode          select;
    private RelNode          orderLimit;

    private Analyzer analyzer;
    private SingleNodePlanner planner;

    public PlanNodeConverter(RelNode root, Analyzer analyzer, SingleNodePlanner planner) {
        this.root = root;
        this.analyzer = analyzer;
        this.planner = planner;
    }

    class QBVisitor extends RelVisitor {
        public void handle(Filter filter) {
            RelNode child = filter.getInput();
            if (child instanceof Aggregate && !((Aggregate) child).getGroupSet().isEmpty()) {
                PlanNodeConverter.this.having = filter;
            } else {
                PlanNodeConverter.this.where = filter;
            }
        }

        public void handle(Project project) {
            if (PlanNodeConverter.this.select == null) {
                PlanNodeConverter.this.select = project;
            } else {
                PlanNodeConverter.this.from = project;
            }
        }

        public void handle(TableFunctionScan tableFunctionScan) {
            if (PlanNodeConverter.this.select == null) {
                PlanNodeConverter.this.select = tableFunctionScan;
            } else {
                PlanNodeConverter.this.from = tableFunctionScan;
            }
        }

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {

            if (node instanceof TableScan) {
                PlanNodeConverter.this.from = node;
            } else if (node instanceof Filter) {
                handle((Filter) node);
            } else if (node instanceof Project) {
                handle((Project) node);
            } else if (node instanceof TableFunctionScan) {
                handle((TableFunctionScan) node);
            } else if (node instanceof Join) {
                PlanNodeConverter.this.from = node;
            } else if (node instanceof Union) {
                PlanNodeConverter.this.from = node;
            } else if (node instanceof Aggregate) {
                PlanNodeConverter.this.groupBy = (Aggregate) node;
            } else if (node instanceof Sort || node instanceof Exchange) {
                if (PlanNodeConverter.this.select != null) {
                    PlanNodeConverter.this.from = node;
                } else {
                    PlanNodeConverter.this.orderLimit = node;
                }
            }
            /*
             * once the source node is reached; stop traversal for this QB
             */
            if (PlanNodeConverter.this.from == null) {
                node.childrenAccept(this);
            }
        }

    }

    private Type calciteTypeToDorisType(RelDataType type) {
        String typeName = type.getSqlTypeName().getName();
        if (typeName.equals("INTEGER")) {
            return Type.INT;
        } else if (typeName.equals("CHAR")) {
            return Type.CHAR;
        } else {
            String msg = typeName + " can't supported";
            throw new UnsupportedOperationException(msg);
        }
    }

    private TupleDescriptor createTupleDescriptor(TableScan scan, List<Expr> resultExprs, List<String> colLables) throws AnalysisException {
        String dbName = analyzer.getDefaultDb();
        String tblName = scan.getTable().getQualifiedName().get(0).toLowerCase();
        LOG.warn("tableName={}", tblName);
        TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor(tblName);
        tupleDesc.setIsMaterialized(true);
        TableName tableName = new TableName(dbName, tblName);

        Database database = analyzer.getCatalog().getDb(dbName);
        if (database == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        Table table = database.getTable(tblName);
        if (table == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tblName);
        }
        TableRef tableRef = new BaseTableRef(new TableRef(tableName, null), table, tableName);
        tupleDesc.setRef(tableRef);
        tupleDesc.setTable(table);

        for (RelDataTypeField field : scan.deriveRowType().getFieldList()) {
            String colName = field.getKey();
            SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
            slotDesc.setLabel(colName);
            slotDesc.setType(calciteTypeToDorisType(field.getType()));
            slotDesc.setIsMaterialized(true);
            Column column = table.getColumn(colName);
            if (column == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, colName,
                   tableName);
            }
            slotDesc.setColumn(column);

            SlotRef slotRef = new SlotRef(tableName, colName);
            slotRef.setDesc(slotDesc);
            slotRef.setType(slotDesc.getType());
            //slotRef.analyze(analyzer);
            resultExprs.add(slotRef);
            colLables.add(colName);
        }
        tupleDesc.computeMemLayout();


        return tupleDesc;
    }

    private ScanNode convertSource(RelNode r, List<Expr> resultExprs, List<String> colLables)
            throws UserException {
        ScanNode scanNode = null;
        if (r instanceof TableScan) {
            TableScan scan = (TableScan)r;
            TupleDescriptor descriptor = createTupleDescriptor(scan, resultExprs, colLables);
            scanNode = new OlapScanNode(new PlanNodeId(0), descriptor,"OlapScanNode");

            Map<String, PartitionColumnFilter> columnFilters = Maps.newHashMap();
            List<Expr> conjuncts = analyzer.getUnassignedConjuncts(scanNode);

            for (Column column : descriptor.getTable().getBaseSchema()) {
                SlotDescriptor slotDesc = descriptor.getColumnSlot(column.getName());
                if (null == slotDesc) {
                    continue;
                }
                PartitionColumnFilter keyFilter = planner.createPartitionFilter(slotDesc, conjuncts);
                if (null != keyFilter) {
                    columnFilters.put(column.getName(), keyFilter);
                }
            }
            scanNode.setColumnFilters(columnFilters);
            scanNode.init(analyzer);
        } else {
            Preconditions.checkState(false);
        }
        return scanNode;
    }

    public PlanNode convert(List<Expr> resultExprs, List<String> colLabels)  {
        /*
         * 1. Walk RelNode Graph; note from, where, gBy.. nodes.
         */
        new QBVisitor().go(root);

        /*
         * 2. convert from node.
         */
        PlanNode node = null;
        try {
            node = convertSource(from, resultExprs, colLabels);
        } catch (UserException e) {
            e.printStackTrace();
        }
        System.out.println("convert end");

        return node;
    }
}
