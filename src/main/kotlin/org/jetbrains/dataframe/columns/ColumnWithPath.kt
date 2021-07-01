package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.impl.columns.addParentPath
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.depth
import kotlin.reflect.KType

public interface ColumnWithPath<out T> : ColumnReference<T> {

    public val df: DataFrameBase<*>
    public val data: DataColumn<T>
    public val path: ColumnPath
    public val kind: ColumnKind get() = data.kind()
    public val depth: Int get() = path.depth()
    public val name: String get() = data.name
    public val type: KType get() = data.type
    public val hasNulls: Boolean get() = data.hasNulls
    public val parent: ColumnWithPath<*>?
    public fun isGroup(): Boolean = data.isGroup()
    public fun depth(): Int = path.depth()
    public fun <C> getChild(accessor: ColumnReference<C>): ColumnWithPath<C>? = asGroup()?.tryGetColumn(accessor)?.addPath(path + accessor.path(), df)
    public fun getChild(name: String): ColumnWithPath<Any?>? = asGroup()?.tryGetColumn(name)?.addParentPath(path, df)
    public fun getChild(index: Int): ColumnWithPath<Any?>? = asGroup()?.tryGetColumn(index)?.addParentPath(path, df)
    public fun children(): List<ColumnWithPath<Any?>> = if (isGroup()) data.asGroup().columns().map { it.addParentPath(path, df) } else emptyList()
    override fun name(): String = name

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<T> = this

    override fun rename(newName: String): ColumnWithPath<T>
}
