package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        ResourceName resourceName = getResourceName();

        // 1. InvalidLockException Check idk

        if (this.parentContext() != null) {
            LockType pLockType = lockman.getLockType(transaction, this.parentContext().getResourceName());
            if (!LockType.canBeParentLock(pLockType, lockType)) throw new InvalidLockException("LockContext_acquire_01_01");
        }

        if ((lockType.equals(LockType.S) || lockType.equals(LockType.IS)) && this.hasSIXAncestor(transaction)) {
            throw new InvalidLockException("LockContext_acquire_01_01");
        }

        // 2. DuplicateLockRequestException is dealt in lockman.acquire I believe

        // 3. UnsupportedOperationException Check
        if (this.readonly) throw new UnsupportedOperationException("LockContext_acquire_03_01");

        // 4. Run LockManager acquire
        this.lockman.acquire(transaction, resourceName, lockType);

        // 5. Update numChildLocks
        LockContext pointer = this.parent;
        while (pointer != null) {
            Long transNum = transaction.getTransNum();
            pointer.numChildLocks.put(transNum, pointer.numChildLocks.getOrDefault(transNum, 0) + 1);
            pointer = pointer.parent;
        }

        return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

        // 01. UnsupportedOperationException Check if context is readonly
        if (this.readonly) {
            throw new UnsupportedOperationException("LockContext_release_03: This context is read only.");
        }
        // 02. Check if there is a lock on 'name' held by 'transaction'
        if (this.lockman.getLockType(transaction, getResourceName()).equals(LockType.NL)) {
            throw new NoLockHeldException("LockContext_release_01: No lock on this name is held by this transaction.");
        }

        // 03. Check if lock can be released by seeing if there is already a value mapped to the specified key
        if (this.numChildLocks.getOrDefault(transaction.getTransNum(), 0) != 0) {
            throw new InvalidLockException("LockContext_release_02: Lock cannot be released because of multigranularity locking constraints.");
        }

        // 04. Call LockManager.release
        this.lockman.release(transaction, getResourceName());

        // 05. Subtract 1 for numChildLocks on every parent
        LockContext pointer = this.parent;
        while (pointer != null) {
            Long transNum = transaction.getTransNum();
            int numLocks = pointer.numChildLocks.getOrDefault(transNum, 0);
//            // Check if parent has any locks. If not, can't subtract one and subsequently there are no more parent locks, so return
//            if (numLocks == 0) {
//                return;
//            } else {
//                pointer.numChildLocks.put(transNum, numLocks - 1);
//                pointer = pointer.parent;
//            }
            pointer.numChildLocks.put(transNum, numLocks - 1);
            pointer = pointer.parent;
        }


        /*
        List<Lock> allLocks = this.lockman.getLocks(transaction);
        for (Lock currLock : allLocks) {
            ResourceName currLockName = currLock.name;
            LockType currLockType = currLock.lockType;
            if (currLockName.isDescendantOf(this.getResourceName())) {
                // release descendant's lock
                // this.childContext(currLockName.toString()).release(transaction);
                if (!lockman.getLockType(transaction, currLockName).equals(LockType.NL)) {
                    lockman.release(transaction, currLockName);
                    this.childContext(currLockName.toString()).numChildLocks.replace(transaction.getTransNum(), 0);
                }
            }
        }


         */
        //return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // 01. UnsupportedOperationException Check
        if (this.readonly) {
            throw new UnsupportedOperationException("LockContext_promote_01: The context is readonly.");
        }

        LockType thisLockType = this.lockman.getLockType(transaction, getResourceName());

        // 02. Check if there is no lock on 'name' held by 'transaction'
        if (thisLockType.equals(LockType.NL)) {
            throw new NoLockHeldException("LockContext_promote_02: No lock on this name is held by this transaction.");
        }

        // 03. Check if the lock held is equivalent to newLockType already
        if (thisLockType.equals(newLockType)) {
            throw new DuplicateLockRequestException("LockContext_promote_03: Transaction already has a lock of this type.");
        }

        // 04. InvalidLock
        if (!LockType.substitutable(newLockType, thisLockType)) {
            throw new InvalidLockException("LockContext_promote_04: This lock is not promotable by newLockType");
        }

        // 05. promote.
        if (newLockType.equals(LockType.SIX)) {Integer releaseCount = 0;
            List<Lock> allLocks = this.lockman.getLocks(transaction);
            List<ResourceName> releaseNames = new ArrayList<ResourceName>();
            releaseNames.add(getResourceName());


            for (Lock currLock : allLocks) {
                ResourceName currLockName = currLock.name;
                LockType currLockType = currLock.lockType;
                if (currLockName.isDescendantOf(this.getResourceName()) && (currLockType.equals(LockType.S) || currLockType.equals(LockType.IS))) {
                    releaseNames.add(currLock.name);
                    releaseCount += 1;
                    this.childContext(currLockName.toString()).numChildLocks.replace(transaction.getTransNum(), 0);
                }
            }
            this.lockman.acquireAndRelease(transaction, getResourceName(), newLockType, releaseNames);
        }
        else this.lockman.promote(transaction, getResourceName(), newLockType);

        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        // 01. UnsupportedOperationException Check
        if (this.readonly) {
            throw new UnsupportedOperationException("LockContext_escalate_01");
        }

        LockType thisLockType = this.lockman.getLockType(transaction, getResourceName());

        // 02. Check if there is no lock on 'name' held by 'transaction'
        if (thisLockType.equals(LockType.NL)) {
            throw new NoLockHeldException("LockContext_escalate_02: No lock on this name is held by this transaction.");
        }

        // 03. Check if lock type is S or X (leaf node), return if true
        if (thisLockType.equals(LockType.S) || thisLockType.equals(LockType.X)) {
            return;
        }


        Boolean x = false;
        if (thisLockType.equals(LockType.IX) || thisLockType.equals(LockType.SIX)) x = true;
        Integer releaseCount = 0;
        List<Lock> allLocks = this.lockman.getLocks(transaction);
        List<ResourceName> releaseNames = new ArrayList<ResourceName>();
        releaseNames.add(getResourceName());


        for (Lock currLock : allLocks) {
            ResourceName currLockName = currLock.name;
            LockType currLockType = currLock.lockType;
            if (currLockName.isDescendantOf(this.getResourceName()) && !lockman.getLockType(transaction, currLockName).equals(LockType.NL)) {
                releaseNames.add(currLock.name);
                releaseCount += 1;
                this.childContext(currLockName.toString()).numChildLocks.replace(transaction.getTransNum(), 0);
                if (currLockType.equals(LockType.X)) {
                    x = true;
                }
            }
        }

        if (x) {
            if (thisLockType.equals(LockType.S)) {
                lockman.acquireAndRelease(transaction, this.getResourceName(), LockType.SIX, releaseNames);
            }
            else lockman.acquireAndRelease(transaction, this.getResourceName(), LockType.X, releaseNames);
        }
        else lockman.acquireAndRelease(transaction, this.getResourceName(), LockType.S, releaseNames);

        if(!this.numChildLocks.isEmpty()) {
            if (this.numChildLocks.get(transaction.getTransNum()) - releaseCount != 0) {
                throw new NoLockHeldException("releaseCount doesn't match!");
            }
            this.numChildLocks.replace(transaction.getTransNum(), 0);
        }

        /*
        for (Lock currLock : allLocks) {
            ResourceName currLockName = currLock.name;
            LockType currLockType = currLock.lockType;
            if (currLockName.isDescendantOf(this.getResourceName()) && !lockman.getLockType(transaction, currLockName).equals(LockType.NL)) {
                // release descendant's lock
                //this.childContext(currLockName.toString()).release(transaction);
                lockman.release(transaction, currLockName);
                this.childContext(currLockName.toString()).numChildLocks.replace(transaction.getTransNum(), 0);
                releaseCount += 1;
                if (currLockType.equals(LockType.X)) {
                    x = true;
                }
            }
        }
        lockman.acquireAndRelease(transaction, this.getResourceName(), LockType.X, releaseNames);
        // should be 0

        if(!this.numChildLocks.isEmpty()) {
            if (this.numChildLocks.get(transaction.getTransNum()) - releaseCount != 0) {
                throw new NoLockHeldException("releaseCount doesn't match!");
            }
            this.numChildLocks.replace(transaction.getTransNum(), 0);
        }



        release(transaction);
        if (x) {
            acquire(transaction, LockType.X);
        }
        else acquire(transaction, LockType.S);


         */
        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return this.lockman.getLockType(transaction, getResourceName());
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType expLockType = getExplicitLockType(transaction);
        if (!expLockType.equals(LockType.NL)) {
            return expLockType;
        }
        else if (this.parent == null) {
            return expLockType;
        }
        else if (this.parentContext().getEffectiveLockType(transaction).equals(LockType.S)) {
            return LockType.S;
        }
        else if (this.parentContext().getExplicitLockType(transaction).equals(LockType.NL)) {
            return LockType.NL;
        }


        // if there is an S in any ancestors, return S

        // if the parent is IS and you don't have children, return S
/*
        if (this.parent != null) {
            return this.parent.getEffectiveLockType(transaction);
        }
        else {
            return expLockType;
        }

 */
        return LockType.NL;
    }


    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext pointer = this.parent;
        while (pointer != null) {
            if (this.lockman.getLockType(transaction, pointer.getResourceName()).equals(LockType.SIX)) {
                return true;
            }
            pointer = pointer.parent;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        // Get a list of all current locks
        List<Lock> allLocks = this.lockman.getLocks(transaction);
        ResourceName name = getResourceName();
        List<ResourceName> sisDesc = new ArrayList<ResourceName>();

        // Iterate through them and keep only the ones that are (S || IS) && descendants of current transaction context
        for (Lock currLock : allLocks) {
            ResourceName currLockName = currLock.name;
            LockType currLockType = currLock.lockType;
            if ((currLockType.equals(LockType.S) || currLockType.equals(LockType.IS)) && (currLockName.isDescendantOf(this.getResourceName()))) {
                sisDesc.add(currLockName);
            }
        }
        return sisDesc;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

