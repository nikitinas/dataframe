package org.jetbrains.dataframe.impl.columns.missing

import org.jetbrains.dataframe.columns.ValueColumn

internal class MissingValueColumn<T> : MissingDataColumn<T>(), ValueColumn<T> {

    override fun distinct() = throw UnsupportedOperationException()
}
