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

package com.vertica.spark.config

import ch.qos.logback.classic.Level

/**
 * Represents configuration for a filestore used by the connector.
 *
 * There is not currently much user configuration for the filestore beyond the address to connect to.
 * @param address Address of the distributed filestore, ie HDFS, to connect to.
 * @param logLevel One of [OFF,DEBUG,INFO,WARN,ERR] depending on configured level of logging desired.
 */
final case class FileStoreConfig(address: String, override val logLevel: Level) extends GenericConfig
