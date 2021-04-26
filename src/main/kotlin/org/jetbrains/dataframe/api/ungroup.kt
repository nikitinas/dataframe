package org.jetbrains.dataframe

import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.removeAt
import kotlin.reflect.KProperty

fun <T> DataFrame<T>.ungroup(vararg columns: KProperty<*>) = ungroup { columns.toColumns() }
fun <T> DataFrame<T>.ungroup(vararg columns: String) = ungroup { columns.toColumns() }
fun <T> DataFrame<T>.ungroup(vararg columns: Column) = ungroup { columns.toColumns() }

fun <T, C> DataFrame<T>.ungroup(selector: ColumnsSelector<T, C>): DataFrame<T> {

    return move { selector.toColumns().children() }
            .into { it.path.removeAt(it.path.size - 2) }
}