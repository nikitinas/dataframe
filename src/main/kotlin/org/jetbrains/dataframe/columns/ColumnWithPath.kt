package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.addParentPath
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.impl.columns.depth
import kotlin.reflect.KType

interface ColumnWithPath<out T> : ColumnReference<T> {

    val df: DataFrameBase<*>
    val data: DataColumn<T>
    val path: ColumnPath
    val kind: ColumnKind get() = data.kind()
    val depth: Int get() = path.depth()
    val name: String get() = data.name
    val type: KType get() = data.type
    val hasNulls: Boolean get() = data.hasNulls
    val parent: ColumnWithPath<*>?
    fun isGroup() = data.isGroup()
    fun depth() = path.depth()
    fun <C> getChild(accessor: ColumnReference<C>): ColumnWithPath<C>? = asGroup()?.tryGetColumn(accessor)?.addPath(path + accessor.path(), df)
    fun getChild(name: String) = asGroup()?.tryGetColumn(name)?.addParentPath(path, df)
    fun getChild(index: Int) = asGroup()?.tryGetColumn(index)?.addParentPath(path, df)
    fun children() = if (isGroup()) data.asGroup().columns().map { it.addParentPath(path, df) } else emptyList()
    override fun name() = name

    override fun resolveSingle(context: ColumnResolutionContext) = this

    override fun rename(newName: String): ColumnWithPath<T>
}