package org.jetbrains.dataframe

public fun <T> DataFrame<T>.copy(): DataFrame<T> = columns().asDataFrame<T>()
