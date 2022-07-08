// (c) Copyright [2020-2021] Micro Focus or one of its affiliates.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.vertica.spark.util.reflections

import com.vertica.spark.config.ReadConfig
import com.vertica.spark.datasource.core.DSConfigSetupInterface
import com.vertica.spark.datasource.v2.{VerticaScanBuilder, VerticaScanBuilderWithPushdown}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation

class ReflectionTools {
  def makeScanBuilderWithPushDown(config: ReadConfig, readSetupInterface: DSConfigSetupInterface[ReadConfig]): VerticaScanBuilderWithPushdown = {
    classOf[VerticaScanBuilderWithPushdown]
      .getDeclaredConstructor(classOf[ReadConfig], classOf[DSConfigSetupInterface[ReadConfig]])
      .newInstance(config, readSetupInterface)
  }

  def makeScanBuilderWithoutPushDown(config: ReadConfig, readSetupInterface: DSConfigSetupInterface[ReadConfig]): VerticaScanBuilder = {
    classOf[VerticaScanBuilder]
      .getDeclaredConstructor(classOf[ReadConfig], classOf[DSConfigSetupInterface[ReadConfig]])
      .newInstance(config, readSetupInterface)
  }

  def aggregationInvokeMethod[T](aggregation: Aggregation, methodName: String): T = {
    classOf[Aggregation]
      .getDeclaredMethod(methodName)
      .invoke(aggregation)
      .asInstanceOf[T]
  }
}
