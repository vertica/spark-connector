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

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll

//scalastyle:off
class VersionTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {

  it should "compare to bigger version" in {
    assert(Version(11, 1, 5, 3).largerThan(Version(10, 4, 7, 5)))
  }

  it should "compare to smaller version" in {
    assert(Version(11, 1, 5, 3).lessThan(Version(12, 0, 2, 1)))
  }

  it should "compare to smaller or equal versions" in {
    assert(Version(11, 1, 5, 3).lesserOrEqual(Version(11, 1, 5, 3)))
    assert(Version(11, 1, 5, 3).lesserOrEqual(Version(11, 2, 5, 3)))
  }

  it should "compare to bigger or equal versions" in {
    assert(Version(11, 1, 5, 3).largerOrEqual(Version(11, 1, 5, 3)))
    assert(Version(11, 1, 5, 3).largerOrEqual(Version(11, 1, 5, 2)))
  }
}
