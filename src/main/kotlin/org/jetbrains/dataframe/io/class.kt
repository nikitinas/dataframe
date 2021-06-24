package org.jetbrains.dataframe.io

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.stubs.DataFrameToListNamedStub
import org.jetbrains.dataframe.stubs.DataFrameToListTypedStub

public inline fun <reified C> AnyFrame.writeClass(): DataFrameToListTypedStub {
    check(C::class.java.isInterface) { "Interface class is expected" }
    return DataFrameToListTypedStub(this, C::class)
}

public fun AnyFrame.writeClass(className: String): DataFrameToListNamedStub = DataFrameToListNamedStub(this, className)
