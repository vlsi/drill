/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.logical;

import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.common.DrillRelNode;
import org.apache.calcite.plan.Convention;

import java.util.function.Supplier;

/**
 * Relational expression that is implemented in Drill.
 */
@org.apache.calcite.plan.ImplicitTrait(DrillRel.ConventionFactory.class)
public interface DrillRel extends DrillRelNode {

  class ConventionFactory implements Supplier<Convention> {
    @Override
    public Convention get() {
      return DRILL_LOGICAL;
    }
  }

  /** Calling convention for relational expressions that are "implemented" by
   * generating Drill logical plans. */
  Convention DRILL_LOGICAL = new Convention.Impl("LOGICAL", DrillRel.class);

  LogicalOperator implement(DrillImplementor implementor);
}
