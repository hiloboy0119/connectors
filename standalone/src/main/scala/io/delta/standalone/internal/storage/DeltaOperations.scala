/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.internal.storage

import io.delta.standalone.SaveMode
import io.delta.standalone.internal.util.JsonUtils
import io.delta.standalone.types.StructType

/**
 * Exhaustive list of operations that can be performed on a Delta table. These operations are
 * tracked as the first line in delta logs, and power `DESCRIBE HISTORY` for Delta tables.
 */
object DeltaOperations {

  /**
   * An operation that can be performed on a Delta table.
   * @param name The name of the operation.
   */
  sealed abstract class Operation(val name: String) {
    val parameters: Map[String, Any]

    lazy val jsonEncodedValues: Map[String, String] = parameters.mapValues(JsonUtils.toJson(_))
  }

  /** Recorded during batch inserts. Predicates can be provided for overwrites. */
  case class Write(
                    mode: SaveMode,
                    partitionBy: Option[Seq[String]] = None,
                    predicate: Option[String] = None) extends Operation("WRITE") {
    override val parameters: Map[String, Any] = Map("mode" -> mode.name()) ++
      partitionBy.map("partitionBy" -> JsonUtils.toJson(_)) ++
      predicate.map("predicate" -> _)
  }

  case class UpdateSchema(oldSchema: StructType, newSchema: StructType)
    extends Operation("UPDATE SCHEMA") {
    override val parameters: Map[String, Any] = Map(
      "oldSchema" -> JsonUtils.toJson(oldSchema),
      "newSchema" -> JsonUtils.toJson(newSchema))
  }
}