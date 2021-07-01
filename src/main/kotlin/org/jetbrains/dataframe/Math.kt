package org.jetbrains.dataframe

import java.math.RoundingMode

public fun Double.round(places: Int): Double {
    require(places >= 0)
    return toBigDecimal().setScale(places, RoundingMode.HALF_UP).toDouble()
}

public fun <T : Comparable<T>> T.between(left: T, right: T, includeBoundaries: Boolean = true): Boolean =
    if (includeBoundaries) this in left..right
    else this > left && this < right
