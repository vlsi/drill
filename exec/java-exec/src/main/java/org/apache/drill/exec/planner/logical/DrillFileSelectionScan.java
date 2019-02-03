package org.apache.drill.exec.planner.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.drill.exec.store.dfs.FileSelection;

public class DrillFileSelectionScan extends TableScan {
    private final FileSelection fileSelection;

    public DrillFileSelectionScan(RelOptCluster cluster, RelOptTable table, FileSelection fileSelection) {
        super(cluster, cluster.traitSetOf(Convention.NONE), table);
        this.fileSelection = fileSelection;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("fileSelection", fileSelection);
    }
}
