package org.jetbrains.dataframe

public fun <T> DataRow<T>.transpose(): DataFrame<*> = dataFrameOf(owner.columnNames().toColumn(), values.toColumnGuessType())
