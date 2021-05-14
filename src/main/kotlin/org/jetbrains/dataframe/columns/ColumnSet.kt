package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.ColumnResolutionContext

/**
 * A set of columns. Used in column selectors
 * @param C common type of columns
 */
interface ColumnSet<out C> {

    fun resolve(context: ColumnResolutionContext): List<ColumnWithPath<C>>
}