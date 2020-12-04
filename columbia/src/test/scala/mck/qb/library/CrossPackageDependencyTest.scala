package mck.qb.library

import mck.qb.library.sample.CrossPackageDependency
import org.junit.Assert._
import org.junit.Test

class CrossPackageDependencyTest {
  @Test def testAlwaysReturnsTrue() = {
    assertTrue("alwaysReturnsTrue 'true'",CrossPackageDependency.alwaysReturnsTrue())
  }
}
