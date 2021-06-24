package org.jetbrains.dataframe

public fun <C : Comparable<C>> Sequence<C?>.indexOfMin(): Int {
    val iterator = iterator()
    if (!iterator.hasNext()) return -1
    var value = iterator.next()
    var index = 0
    while (value == null) {
        if (!iterator.hasNext()) return -1
        value = iterator.next()
        index++
    }
    var min: C = value
    var minIndex = index
    if (!iterator.hasNext()) return minIndex
    do {
        val v = iterator.next()
        index++
        if (v != null && min > v) {
            min = v
            minIndex = index
        }
    } while (iterator.hasNext())
    return minIndex
}

public fun <C : Comparable<C>> Sequence<C?>.indexOfMax(): Int {
    val iterator = iterator()
    if (!iterator.hasNext()) return -1
    var value = iterator.next()
    var index = 0
    while (value == null) {
        if (!iterator.hasNext()) return -1
        value = iterator.next()
        index++
    }
    var max: C = value
    var maxIndex = index
    if (!iterator.hasNext()) return maxIndex
    do {
        val v = iterator.next()
        index++
        if (v != null && max < v) {
            max = v
            maxIndex = index
        }
    } while (iterator.hasNext())
    return maxIndex
}
