package org.jetbrains.dataframe.io

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.dataFrameOf
import org.jetbrains.dataframe.getType
import org.junit.Test

class TypeInferenceTest {

    open class A

    private class B : A()

    @Test
    fun `private subtypes`(){
        val df = dataFrameOf("col")(B(), B())
        df["col"].type() shouldBe getType<A>()
    }
}