package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held


    /**
     * Compatibility Matrix
     * (Boolean value in cell answers is `left` compatible with `top`?)
     *
     *     | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  T  |  T  |  T  |  T  |  T
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |  T  |  T  |  T  |  T  | 1T  | 1F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |  T  |  T  |  T  |  F  | 1F  | 1F
     * ----+-----+-----+-----+-----+-----+-----
     * S   |  T  |  T  |  F  |  T  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |  T  | 1T  |  1F |  F  |  1F | 1F
     * ----+-----+-----+-----+-----+-----+-----
     * X   |  T  | 1F  |  1F |  F  |  1F |  F
     * ----+-----+-----+-----+-----+-----+-----
     *
     * The filled in cells are covered by the public tests.
     * You can expect the blank cells to be covered by the hidden tests!
     * Hint: I bet the notes might have something useful for this...
     *
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement

        if (a.equals(NL) || b.equals(NL)) return true;

        switch (a) {
            case S: return b.equals(IS) || b.equals(S);
            case X: return false;
            case IS: return !b.equals(X);
            case IX: return b.equals(IS) || b.equals(IX);
            case SIX: return b.equals(IS);
            default: throw new UnsupportedOperationException("It's not a lock type.");
        }
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }


    /**
     * Parent Matrix
     * (Boolean value in cell answers can `left` be the parent of `top`?)
     *
     *     | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |  T  |  T  |  F  |  T  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |  T  |  T  |  T  |  T  |  T  |  T
     * ----+-----+-----+-----+-----+-----+-----
     * S   |  T  | 1F  | 1F  | 1F  |  1F | 1F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |  T  | 1F  | 1T  | 1F  | 1F  | 1T
     * ----+-----+-----+-----+-----+-----+-----
     * X   |  T  | 1F  | 1F  | 1F  | 1F  | 1F
     * ----+-----+-----+-----+-----+-----+-----
     *
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement

        if (childLockType.equals(NL) || parentLockType.equals(IX))
            return true;

        switch (parentLockType) {
            case NL: return false;
            case S: return false; // todo for hidden test
//            case X: return childLockType.equals(S) || childLockType.equals(X);
            case X: return false;
            case IS: return childLockType.equals(S) || childLockType.equals(IS);
            case SIX: return childLockType.equals(IX) || childLockType.equals(X);
            default: throw new UnsupportedOperationException("bad parent lock type");
        }
    }


    /**
     * Substitutability Matrix
     * (Values along left are `substitute`, values along top are `required`)
     *
     *     | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IS  | 1T  |  T  |  F  |  F  | 1F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  | 1T  |  T  |  T  |  F  | 1F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * S   | 1T  | 1F  | 1F  |  T  | 1F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX | 1T  | 1F  | 2F  |  T  | 1T  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * X   | 1T  | 1F  | 1F  |  T  | 1F  |  T
     * ----+-----+-----+-----+-----+-----+-----
     *
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (required) {
            case NL: return true;
            case S: return substitute.equals(SIX) || substitute.equals(X)
                    || substitute.equals(S);
            case X: return substitute.equals(X);
            case IS: return substitute.equals(IS) || substitute.equals(IX);
            case IX: return substitute.equals(IX);
            case SIX: return substitute.equals(SIX);
            default: throw new UnsupportedOperationException("bad required lock type");
        }
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

