package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.DataColumn

public fun <T> AnyCol.castTo(): DataColumn<T> = this as DataColumn<T>
