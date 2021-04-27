package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.AnyCol
import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.getColumn
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.isGroup
import org.jetbrains.dataframe.impl.columns.typed
import kotlin.reflect.KProperty

interface ColumnDefinition<out T> : ColumnReference<T> {

    operator fun getValue(thisRef: Any?, property: KProperty<*>) = this

    fun <C> changeType() = this as ColumnDefinition<C>
}

fun <T> ColumnReference<T>.definition(): ColumnDefinition<T> = when(this){
    is ColumnDefinition<T> -> this
    else -> ColumnDefinitionImpl(path())
}

internal class ColumnDefinitionImpl<T>(val path: ColumnPath) : ColumnDefinition<T> {

    override fun name() = path.last()

    override fun path() = path

    constructor(vararg path: String): this(path.toList())

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<T>? {
        var df = context.df
        var col : AnyCol? = null
        for(colName in path){
            col = df.getColumn<Any?>(colName, context.unresolvedColumnsPolicy) ?: return null
            if(col.isGroup())
                df = col.asGroup().df
        }
        return col?.typed<T>()?.addPath(path, context.df)
    }
}