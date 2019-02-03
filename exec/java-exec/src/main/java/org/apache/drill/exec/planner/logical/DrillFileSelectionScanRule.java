package org.apache.drill.exec.planner.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;

public class DrillFileSelectionScanRule extends ConverterRule {
    public static final DrillFileSelectionScanRule INSTANCE = new DrillFileSelectionScanRule();

    public DrillFileSelectionScanRule() {
        super(DrillFileSelectionScan.class, Convention.NONE, DrillRel.DRILL_LOGICAL, "DrillFileSelectionScanRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        TableScan scan = (TableScan) rel;
        return new DrillScanRel(
                rel.getCluster(),
                rel.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
                scan.getTable());
    }
}
