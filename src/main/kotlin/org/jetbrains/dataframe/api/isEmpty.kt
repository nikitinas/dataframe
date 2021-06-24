package org.jetbrains.dataframe

public fun AnyRow.isEmpty(): Boolean = owner.columns().all { it[index] == null }
public fun AnyFrame.isEmpty(): Boolean = ncol == 0 || nrow == 0
public fun AnyRow.isNotEmpty(): Boolean = !isEmpty()
public fun AnyFrame.isNotEmpty(): Boolean = !isEmpty()
