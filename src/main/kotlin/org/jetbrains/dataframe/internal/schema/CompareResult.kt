package org.jetbrains.dataframe.internal.schema

internal enum class CompareResult {
    Equals,
    IsSuper,
    IsDerived,
    None;

    fun isSuperOrEqual() = this == Equals || this == IsSuper

    fun isEqual() = this == Equals

    fun combine(other: CompareResult) =
            when (this) {
                Equals -> other
                None -> None
                IsDerived -> if (other == Equals || other == IsDerived) this else None
                IsSuper -> if (other == Equals || other == IsSuper) this else None
            }

    companion object {
        fun compareNullability(thisIsNullable: Boolean, otherIsNullable: Boolean) = when {
            thisIsNullable == otherIsNullable -> Equals
            thisIsNullable -> IsSuper
            else -> IsDerived
        }
    }
}