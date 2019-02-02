package org.apache.drill.exec.planner.logical;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.List;

/**
 * Logical MultiJoin implemented in Drill.
 */
@org.apache.calcite.plan.ImplicitTrait(DrillRel.ConventionFactory.class)
public class DrillMultiJoinRel extends MultiJoin {
    public DrillMultiJoinRel(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, RexNode joinFilter, RelDataType rowType, boolean isFullOuterJoin, List<RexNode> outerJoinConditions, List<JoinRelType> joinTypes, List<ImmutableBitSet> projFields, ImmutableMap<Integer, ImmutableIntList> joinFieldRefCountsMap, RexNode postJoinFilter) {
        super(cluster, traits, inputs, joinFilter, rowType, isFullOuterJoin, outerJoinConditions, joinTypes, projFields, joinFieldRefCountsMap, postJoinFilter);
        assert traits.contains(DrillRel.DRILL_LOGICAL);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DrillMultiJoinRel(
                getCluster(),
                traitSet,
                inputs,
                joinFilter,
                rowType,
                isFullOuterJoin,
                outerJoinConditions,
                joinTypes,
                projFields,
                joinFieldRefCountsMap,
                postJoinFilter);
    }

    public RelNode accept(RexShuttle shuttle) {
        RexNode joinFilter = shuttle.apply(this.joinFilter);
        List<RexNode> outerJoinConditions = shuttle.apply(this.outerJoinConditions);
        RexNode postJoinFilter = shuttle.apply(this.postJoinFilter);

        if (joinFilter == this.joinFilter
                && outerJoinConditions == this.outerJoinConditions
                && postJoinFilter == this.postJoinFilter) {
            return this;
        }

        return new DrillMultiJoinRel(
                getCluster(),
                traitSet,
                inputs,
                joinFilter,
                rowType,
                isFullOuterJoin,
                outerJoinConditions,
                joinTypes,
                projFields,
                joinFieldRefCountsMap,
                postJoinFilter);
    }

}
