package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.ColumnKind

public interface ValueColumn<T> : DataColumn<T> {

    override fun distinct(): ValueColumn<T>

    override fun kind(): ColumnKind = ColumnKind.Value
}
