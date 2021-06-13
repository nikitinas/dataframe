package org.jetbrains.dataframe

import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.asTable
import org.jetbrains.dataframe.columns.values
import kotlin.reflect.KType

inline fun <T, reified C> DataFrame<T>.mergeRows(noinline selector: ColumnsSelector<T, C>) = mergeRows(this, selector, getType<C>())

fun <T, C> mergeRows(df: DataFrame<T>, selector: ColumnsSelector<T, C>, type: KType): DataFrame<T> {

    val listType = List::class.createTypeWithArgument(type)
    return df.groupBy { except(selector) }.mapNotNullGroups {
        val updated = update(selector).suggestTypes(List::class to listType).with2 { row, column ->
            if(row.index > 0) null
            else when(column.kind()) {
                ColumnKind.Value -> column.toList()
                ColumnKind.Group -> column.asGroup().df
                ColumnKind.Frame -> column.asTable().values.union()
            }
        }
        updated[0..0]
    }.ungroup()
}