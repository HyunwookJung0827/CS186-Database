package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

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


        if (requestType.equals(LockType.NL)) return;
        else if (explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)) { // CASE 2
            // We need SIX
            lockContext.promote(transaction, LockType.SIX);
        }
        else if (explicitLockType.equals(LockType.NL)) {
            if (requestType.equals(LockType.S)){
                if (effectiveLockType.equals(LockType.NL)) {
                    // Give all of this ancestors IS locks
                    ISLocksForAncestors(parentContext);
                } else if (effectiveLockType.equals(LockType.S)) {
                    SLocksForAncestors(parentContext);
                }
                lockContext.acquire(transaction, LockType.S);
            }
            else if (requestType.equals(LockType.X)){
                if (effectiveLockType.equals(LockType.NL)) {
                    // Give all of this ancestors IS locks
                    IXLocksForAncestors(parentContext);
                } else if (effectiveLockType.equals(LockType.X)) {
                    XLocksForAncestors(parentContext);
                }
                lockContext.acquire(transaction, LockType.X);
            }
        }
        else if (requestType.equals(LockType.S) && explicitLockType.equals(LockType.X)) return ;

        else if (explicitLockType.equals(requestType)) {
            lockContext.escalate(transaction);
        }
        else if (effectiveLockType.equals(requestType)) {
            lockContext.escalate(transaction);
        }
        else if (LockType.substitutable(explicitLockType, requestType)) {
            lockContext.promote(transaction, requestType);
        }
        else if (requestType.equals(LockType.S)){
            if (effectiveLockType.equals(LockType.S)) {
                // Give all of this ancestors IS locks
                SLocksForAncestors(parentContext);
                lockContext.promote(transaction, LockType.S);
            } else {
                // Give all of this ancestors IS locks
                ISLocksForAncestors(parentContext);
                lockContext.escalate(transaction);
            }

        }
        else if (requestType.equals(LockType.X)){
            if (effectiveLockType.equals(LockType.X)) {
                // Give all of this ancestors IS locks
                XLocksForAncestors(parentContext);
                lockContext.promote(transaction, LockType.X);
            } else {
                // Give all of this ancestors IS locks
                IXLocksForAncestors(parentContext);
                lockContext.promote(transaction, requestType);
            }
        }

        else if (explicitLockType.isIntent()) { // CASE 3
            lockContext.escalate(transaction);
            return;
        }



        return;
    }

    // TODO(proj4_part2) add any helper methods you want
    public static void ISLocksForAncestors(LockContext parentContext) {
        TransactionContext transaction = TransactionContext.getTransaction();
        Iterator<LockContext> ancestorsIter = getAncestors(parentContext);
        while (ancestorsIter.hasNext()) {
            LockContext ancestor = ancestorsIter.next();
            LockType ancestorExplicitLockType = ancestor.getExplicitLockType(transaction);
            if (ancestorExplicitLockType.equals(LockType.NL)) {
                ancestor.acquire(transaction, LockType.IS);
            }
        }
    }

    public static void SLocksForAncestors(LockContext parentContext) {
        TransactionContext transaction = TransactionContext.getTransaction();
        Iterator<LockContext> ancestorsIter = getAncestors(parentContext);
        while (ancestorsIter.hasNext()) {
            LockContext ancestor = ancestorsIter.next();
            LockType ancestorExplicitLockType = ancestor.getExplicitLockType(transaction);
            if (ancestorExplicitLockType.equals(LockType.NL) || ancestorExplicitLockType.equals(LockType.IS)) {
                ancestor.acquire(transaction, LockType.S);
            }
        }
    }
    public static void IXLocksForAncestors(LockContext parentContext) {
        TransactionContext transaction = TransactionContext.getTransaction();
        Iterator<LockContext> ancestorsIter = getAncestors(parentContext);
        while (ancestorsIter.hasNext()) {
            LockContext ancestor = ancestorsIter.next();
            LockType ancestorExplicitLockType = ancestor.getExplicitLockType(transaction);
            if (ancestorExplicitLockType.equals(LockType.NL)) {
                ancestor.acquire(transaction, LockType.IX);
            }
            else if (ancestorExplicitLockType.equals(LockType.IS)) {
                ancestor.promote(transaction, LockType.IX);
            }
        }
    }

    public static void XLocksForAncestors(LockContext parentContext) {
        TransactionContext transaction = TransactionContext.getTransaction();
        Iterator<LockContext> ancestorsIter = getAncestors(parentContext);
        while (ancestorsIter.hasNext()) {
            LockContext ancestor = ancestorsIter.next();
            LockType ancestorExplicitLockType = ancestor.getExplicitLockType(transaction);
            if (ancestorExplicitLockType.equals(LockType.NL)) {
                ancestor.acquire(transaction, LockType.X);
            }
            else {
                ancestor.promote(transaction, LockType.X);
            }
        }
    }
    public static Iterator<LockContext> getAncestors(LockContext parentContext) {
        ArrayList<LockContext> ancestorArray = new ArrayList<LockContext>();
        LockContext ancestor = parentContext;
        while (ancestor != null) {
            ancestorArray.add(ancestor);
            ancestor = ancestor.parentContext();
        }
        Collections.reverse(ancestorArray);
        return ancestorArray.iterator();
    }

    public static Iterator<LockContext> getDescendants(LockContext parentContext) {
        //???
        ArrayList<LockContext> ancestorArray = new ArrayList<LockContext>();
        LockContext ancestor = parentContext;
        while (ancestor != null) {
            ancestorArray.add(ancestor);
            ancestor = ancestor.parentContext();
        }
        Collections.reverse(ancestorArray);
        return ancestorArray.iterator();
    }
}
