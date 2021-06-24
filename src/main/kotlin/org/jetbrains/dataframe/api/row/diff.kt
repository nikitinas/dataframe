package org.jetbrains.dataframe

public inline fun <T, reified R : Number> DataRow<T>.diff(selector: RowSelector<T, R>): Number = when (R::class) {
    Double::class -> prev?.let { (selector(this) as Double) - (selector(it) as Double) } ?: .0
    Int::class -> prev?.let { (selector(this) as Int) - (selector(it) as Int) } ?: 0
    Long::class -> prev?.let { (selector(this) as Long) - (selector(it) as Long) } ?: 0
    else -> throw NotImplementedError()
}
