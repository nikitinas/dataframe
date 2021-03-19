package org.jetbrains.dataframe.impl.codeGen

internal enum class CompareResult {
    Equals,
    IsSuper,
    IsDerived,
    None;

    fun isSuperOrEqual() = this == Equals || this == IsSuper

    fun isDerivedOrEqual() = this == Equals || this == IsDerived

    fun isEqual() = this == Equals

    fun combine(other: CompareResult) =
            when (this) {
                Equals -> other
                None -> this
                IsDerived -> if (other == Equals || other == IsDerived) this else None
                IsSuper -> if (other == Equals || other == IsSuper) this else None
            }
}