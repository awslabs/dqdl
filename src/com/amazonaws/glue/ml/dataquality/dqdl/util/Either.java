/*
 * Either.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.util;

/*
 * This is a Java port of Scala's Either.
 * This can encapsulate two return types in its signature. "Left" and "Right"
 * "Left" is intended for holding any error messages. "Right" will contain the actual return value otherwise.
 * It cannot contain both at the same time. This is a way for us to not resort to using exceptions for control flow.
 */
public final class Either<L, R> {
    private final L left;
    private final R right;

    private Either(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public boolean isLeft() {
        return left != null;
    }

    public L getLeft() {
        return left;
    }

    public boolean isRight() {
        return right != null;
    }

    public R getRight() {
        return right;
    }

    public static <L, R> Either<L, R> fromLeft(L left) {
        return new Either<>(left, null);
    }

    public static <L, R> Either<L, R> fromRight(R right) {
        return new Either<>(null, right);
    }
}
