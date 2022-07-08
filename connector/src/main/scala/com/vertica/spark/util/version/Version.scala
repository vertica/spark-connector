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

package com.vertica.spark.util.version

/**
 * A class representing a version string of format major.minor.patch-hotfix.
 * Only digits are allowed for minor, patch, and hotfix.
 * */
case class Version(major: Int, minor: Int = 0, servicePack: Int = 0, hotfix: Int = 0) extends Ordered[Version] {

  def largerOrEqual(version: Version): Boolean = this.compare(version) >= 0

  def largerThan(version: Version): Boolean = this.compare(version) > 0

  def lesserOrEqual(version: Version): Boolean = this.compare(version) <= 0

  def lessThan(version: Version): Boolean = this.compare(version) < 0

  def isEquals(version: Version): Boolean =  this.compare(version) == 0

  override def toString: String = s"${major}.${minor}.${servicePack}-${hotfix}"

  override def compare(that: Version): Int =
    (this.major * 1000 + this.minor * 100 + this.servicePack * 10 + this.hotfix) -
      (that.major * 1000 + that.minor * 100 + that.servicePack * 10 + that.hotfix)
}
