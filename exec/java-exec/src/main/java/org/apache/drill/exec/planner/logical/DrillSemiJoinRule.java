package org.apache.drill.exec.planner.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.SemiJoin;

public class DrillSemiJoinRule extends ConverterRule {
    public static final DrillSemiJoinRule INSTANCE = new DrillSemiJoinRule();

    public DrillSemiJoinRule() {
        super(SemiJoin.class, Convention.NONE, DrillRel.DRILL_LOGICAL, "DrillSemiJoinRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        SemiJoin join = (SemiJoin) rel;
        return new DrillSemiJoinRel(
                rel.getCluster(),
                rel.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
                convert(join.getLeft(), join.getLeft().getTraitSet().plus(DrillRel.DRILL_LOGICAL)),
                convert(join.getRight(), join.getRight().getTraitSet().plus(DrillRel.DRILL_LOGICAL)),
                join.getCondition(),
                join.getLeftKeys(),
                join.getRightKeys());
    }
}
