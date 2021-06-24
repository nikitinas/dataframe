package org.jetbrains.dataframe

import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.removeAt
import kotlin.reflect.KProperty

public fun <T> DataFrame<T>.ungroup(vararg columns: KProperty<*>): DataFrame<T> = ungroup { columns.toColumns() }
public fun <T> DataFrame<T>.ungroup(vararg columns: String): DataFrame<T> = ungroup { columns.toColumns() }
public fun <T> DataFrame<T>.ungroup(vararg columns: Column): DataFrame<T> = ungroup { columns.toColumns() }

public fun <T, C> DataFrame<T>.ungroup(selector: ColumnsSelector<T, C>): DataFrame<T> {
    return move { selector.toColumns().children() }
        .into { it.path.removeAt(it.path.size - 2) }
}
