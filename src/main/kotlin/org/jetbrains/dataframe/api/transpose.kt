package org.jetbrains.dataframe

fun <T> DataRow<T>.transpose() = dataFrameOf(owner.columnNames().toColumn(), values.toColumnGuessType())