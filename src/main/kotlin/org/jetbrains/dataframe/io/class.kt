package org.jetbrains.dataframe.io

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.stubs.DataFrameToListNamedStub
import org.jetbrains.dataframe.stubs.DataFrameToListTypedStub

inline fun <reified C> AnyFrame.writeClass(): DataFrameToListTypedStub {
    require(C::class.java.isInterface) { "Class must represent an interface."}
    return DataFrameToListTypedStub(this, C::class)
}
fun AnyFrame.writeClass(className: String) = DataFrameToListNamedStub(this, className)