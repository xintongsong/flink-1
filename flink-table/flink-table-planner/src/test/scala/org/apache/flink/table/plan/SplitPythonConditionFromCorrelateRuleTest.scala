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

package org.apache.flink.table.plan

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction
import org.apache.flink.table.utils.TableTestUtil.{streamTableNode, term, unaryNode}
import org.apache.flink.table.utils.{TableFunc2, TableTestBase}
import org.junit.Test

class SplitPythonConditionFromCorrelateRuleTest extends TableTestBase {

  @Test
  def testPythonFunctionInCorrelateCondition(): Unit = {
    val util = streamTestUtil()
    val func = new TableFunc2
    val pyFunc = new PythonScalarFunction("pyFunc")
    val table = util.addTable[(Int, Int, String)]("MyTable", 'a, 'b, 'c)

    val result = table
      .joinLateral(func('c) as ('s, 'l), 'l === 'a && 'c === 's && pyFunc('l, 'l) === 2)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        unaryNode(
          "DataStreamCorrelate",
          streamTableNode(table),
          term("invocation",
            "org$apache$flink$table$utils$TableFunc2$23cdbf205566c9600253be74cab2763e($2)"),
          term("correlate", "table(TableFunc2(c))"),
          term("select", "a", "b", "c", "s", "l"),
          term("rowType",
            "RecordType(INTEGER a, INTEGER b, VARCHAR(65536) c, VARCHAR(65536) s, INTEGER l)"),
          term("joinType", "INNER"),
          term("condition", "true")
        ),
        term("select", "a", "b", "c", "s", "l", "pyFunc(l, l) AS f0")
      ),
      term("select", "a", "b", "c", "s", "l"),
      term("where", "AND(=(f0, 2), AND(=(l, a), =(c, s)))")
    )

    util.verifyTable(result, expected)
  }
}
