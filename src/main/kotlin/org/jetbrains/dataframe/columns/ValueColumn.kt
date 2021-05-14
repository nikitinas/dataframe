package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.ColumnKind

interface ValueColumn<T> : DataColumn<T> {

    override fun distinct(): ValueColumn<T>

    override fun kind() = ColumnKind.Value
}