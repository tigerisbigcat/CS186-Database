package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement

        //The current lock type can effectively substitute the requested type
        //TODO: explicit locktype?
        if (LockType.substitutable(explicitLockType, requestType)) {
            return;
        }
//        if (LockType.substitutable(effectiveLockType, requestType)) {
//            if (LockType.substitutable(explicitLockType, requestType)) {
//                return;
//            }
//        }

        //Requested locktype is NL.
        //TODO: need to release?
        if (requestType.equals(LockType.NL)) {
            //lockContext.release(transaction);
            return;
        }

        // get the parents
        List<LockContext> parentList = new ArrayList<>();
        LockContext parent = lockContext.parent;

        // add parent into a list
        while (parent != null) {
            parentList.add(parent);
            parent = parent.parent;
        }

        // deal with the list at a reverse order
        Collections.reverse(parentList);

        if (requestType.equals(LockType.S)) { //requested locktype is S
            for (LockContext l : parentList) { //update parents
                LockType getParentLockType = l.getExplicitLockType(transaction);
                // if parent no lock, acquire lock IS
                if (getParentLockType.equals(LockType.NL)) {
                    l.acquire(transaction, LockType.IS);
                }
            }

            //acquire for this context
            if (explicitLockType.equals(LockType.NL)) {//currently NL, acquire S
                lockContext.acquire(transaction, LockType.S);
            } else if (explicitLockType.equals(LockType.IS)) {//currently IS, escalate to S
                lockContext.escalate(transaction);
            } else {  //else (currently IX), promote to SIX
                lockContext.promote(transaction, LockType.SIX);
            }
        } else { //requested locktype is X
            for (LockContext l : parentList) { //update parents
                LockType getParentLockType = l.getExplicitLockType(transaction);
                if (getParentLockType.equals(LockType.NL)) { //parent is NL, acquire IX
                    l.acquire(transaction, LockType.IX);
                } else if (getParentLockType.equals(LockType.IS)) { //parent is IS, acquire IX
                    l.promote(transaction, LockType.IX);
                } else if (getParentLockType.equals(LockType.S)){ //parent is S, acquire SIX
                    l.promote(transaction, LockType.SIX);
                }
            }

            //acquiring X for this context
            if (explicitLockType.equals(LockType.NL)) { //currently NL, acquire X
                lockContext.acquire(transaction, LockType.X);
            } else if (explicitLockType.equals(LockType.IS)) { //currently IS, escalate S then promote X
                lockContext.escalate(transaction);
                lockContext.promote(transaction, LockType.X);
            } else if (explicitLockType.equals(LockType.S)) { //currently S, promote X
                lockContext.promote(transaction,LockType.X);
            } else {
                lockContext.escalate(transaction); //currently SIX, promote X
            }
        }
    }

    // TODO(proj4_part2) add any helper methods you want

}

// We suggest breaking up the logic of this method into two phases:
// ensuring that we have the appropriate locks on ancestors,
// and acquiring the lock on the resource.
// You will need to promote in some cases, and escalate in some
// cases (these cases are not mutually exclusive).