package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.DataColumn

fun <T> AnyCol.castTo() = this as DataColumn<T>