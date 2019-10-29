/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex._
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalCorrelate, FlinkLogicalRel}
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RexDefaultVisitor

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Rule that splits [[FlinkLogicalCalc]] which is the upstream of [[FlinkLogicalCorrelate]]
  * and have python Functions condition into two [[FlinkLogicalCalc]]. One [[FlinkLogicalCalc]]
  * which doesn't have python function condition is still the upstream of
  * [[FlinkLogicalCorrelate]], but the other [[[FlinkLogicalCalc]] which only have python function
  * conditions is the downstream of the [[FlinkLogicalCorrelate]].
  */
class SplitPythonConditionFromCorrelateRule
  extends RelOptRule(
    operand(
      classOf[FlinkLogicalCorrelate],
      operand(classOf[FlinkLogicalRel], any),
      operand(classOf[FlinkLogicalCalc], any)),
    "SplitPythonConditionFromCorrelateRule") {
  override def matches(call: RelOptRuleCall): Boolean = {
    val correlate: FlinkLogicalCorrelate = call.rel(0).asInstanceOf[FlinkLogicalCorrelate]
    val right: FlinkLogicalCalc = call.rel(2).asInstanceOf[FlinkLogicalCalc]
    val joinType: JoinRelType = correlate.getJoinType
    joinType == JoinRelType.INNER && containsPythonFunctionCondition(right)
  }

  private def containsPythonFunctionCondition(calc: FlinkLogicalCalc): Boolean = {
    val program = calc.getProgram
    val conditionExistPythonFunction = Option(program.getCondition)
      .map(program.expandLocalRef)
      .exists(containsPythonCall)
    if (conditionExistPythonFunction) {
      true
    } else {
      val child = calc.getInput.asInstanceOf[HepRelVertex].getCurrentRel
      child match {
        case calc: FlinkLogicalCalc => containsPythonFunctionCondition(calc)
        case _ => false
      }
    }
  }

  private def getMergedCalc(calc: FlinkLogicalCalc): FlinkLogicalCalc = {
    val child = calc.getInput.asInstanceOf[HepRelVertex].getCurrentRel
    child match {
      case logicalCalc: FlinkLogicalCalc =>
        val bottomCalc = getMergedCalc(logicalCalc)
        val topCalc = calc
        val topProgram: RexProgram = topCalc.getProgram
        val mergedProgram: RexProgram = RexProgramBuilder
          .mergePrograms(
            topCalc.getProgram,
            bottomCalc.getProgram,
            topCalc.getCluster.getRexBuilder)
        assert(mergedProgram.getOutputRowType eq topProgram.getOutputRowType)
        topCalc.copy(topCalc.getTraitSet, bottomCalc.getInput, mergedProgram)
          .asInstanceOf[FlinkLogicalCalc]
      case _ =>
        calc
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val correlate: FlinkLogicalCorrelate = call.rel(0).asInstanceOf[FlinkLogicalCorrelate]
    val right: FlinkLogicalCalc = call.rel(2).asInstanceOf[FlinkLogicalCalc]
    val rexBuilder = call.builder().getRexBuilder
    val mergedCalc = getMergedCalc(right)
    val mergedCalcProgram = mergedCalc.getProgram
    val input = mergedCalc.getInput

    val joinFilters = RelOptUtil
      .conjunctions(mergedCalcProgram.getCondition)
      .map(_.asInstanceOf[RexLocalRef])
      .map(mergedCalcProgram.expandLocalRef)

    val remainingFilters = joinFilters
      .filter(!containsPythonCall(_))

    val bottomCalcCondition = RexUtil.composeConjunction(
      rexBuilder,
      RexUtil.fixUp(
        rexBuilder,
        remainingFilters,
        RelOptUtil.getFieldTypeList(input.getRowType)))

    val newBottomCalc = new FlinkLogicalCalc(
      mergedCalc.getCluster,
      mergedCalc.getTraitSet,
      input,
      RexProgram.create(
        input.getRowType,
        mergedCalcProgram.getProjectList,
        bottomCalcCondition,
        mergedCalc.getRowType,
        rexBuilder))

    val newCorrelate = new FlinkLogicalCorrelate(
      correlate.getCluster,
      correlate.getTraitSet,
      correlate.getLeft,
      newBottomCalc,
      correlate.getCorrelationId,
      correlate.getRequiredColumns,
      correlate.getJoinType)

    val inputRefRewriter = new InputRefRewriter(
      correlate.getRowType.getFieldCount - mergedCalc.getRowType.getFieldCount)

    val pythonFilters = joinFilters
      .map(_.accept(inputRefRewriter))
      .filter(containsPythonCall)

    val topCalcCondition = RexUtil.composeConjunction(
      rexBuilder,
      RexUtil.fixUp(
        rexBuilder,
        pythonFilters,
        RelOptUtil.getFieldTypeList(newCorrelate.getRowType)))

    val rexProgram = new RexProgramBuilder(newCorrelate.getRowType, rexBuilder).getProgram
    val newTopCalc = new FlinkLogicalCalc(
      newCorrelate.getCluster,
      newCorrelate.getTraitSet,
      newCorrelate,
      RexProgram.create(
        newCorrelate.getRowType,
        rexProgram.getExprList,
        topCalcCondition,
        newCorrelate.getRowType,
        rexBuilder))

    call.transformTo(newTopCalc)
  }
}

/**
  * Because the inputRef is from the upstream calc node of the correlate node,
  * so after the inputRef is pushed to the downstream calc node of the correlate
  * node, the inputRef need to rewrite the index.
  * @param offset the start offset of the inputRef in the downstream calc.
  */
private class InputRefRewriter(offset: Int)
  extends RexDefaultVisitor[RexNode] {

  override def visitInputRef(inputRef: RexInputRef): RexNode = {
    new RexInputRef(inputRef.getIndex + offset, inputRef.getType)
  }

  override def visitCall(call: RexCall): RexNode = {
    call.clone(call.getType, call.getOperands.asScala.map(_.accept(this)))
  }

  override def visitNode(rexNode: RexNode): RexNode = rexNode
}

object SplitPythonConditionFromCorrelateRule {
  val INSTANCE: SplitPythonConditionFromCorrelateRule = new SplitPythonConditionFromCorrelateRule
}

