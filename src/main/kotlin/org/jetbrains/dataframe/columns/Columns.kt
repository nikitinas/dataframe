package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.ColumnResolutionContext

/**
 * A set of columns. Used in column selectors API
 * @param C common type of columns
 */
interface Columns<out C> {

    fun resolve(context: ColumnResolutionContext): List<ColumnWithPath<C>>
}