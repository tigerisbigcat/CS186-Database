package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
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

//    acquire: this method performs an acquire via the underlying LockManager after ensuring
//    that all multigranularity constraints are met. For example, if the transaction has
//    IS(database) and requests X(table), the appropriate exception must be thrown (see
//    comments above method). If a transaction has a SIX lock, then it is redundant for
//    the transaction to have an IS/S lock on any descendant resource. Therefore, in our
//    implementation, we prohibit acquiring an IS/S lock if an ancestor has SIX, and consider
//    this to be an invalid request.
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
        if (readonly) {
            throw new UnsupportedOperationException("Read only!");
        }

        if (parent == null) { //is DB context
            if (lockman.getLockType(transaction, getResourceName()) != null) { //if already have lock on DB
                if (lockman.getLockType(transaction, getResourceName()).equals(lockType)) { //duplicate lock
                    throw new DuplicateLockRequestException("Duplicate lock by name.");
                }
            }
        } else { //is not DB context

            // acquiring an IS/S lock if an ancestor has SIX, and consider
            // this to be an invalid request.
            if (hasSIXAncestor(transaction) && (lockType.equals(LockType.IS)
                    || lockType.equals(LockType.S))) {
                throw new InvalidLockException("ancestor is SIX, invalid type to acquire.");
            }

            //invalid lock type given parent context's lock type
            if (!LockType.canBeParentLock(parent.getExplicitLockType(transaction), lockType)
                    && (!parent.getExplicitLockType(transaction).equals(LockType.NL))) {
                throw new InvalidLockException("no acquire on current parent NL type.");
            }

            LockContext currParent = parent;
            while (currParent != null) {
                currParent.numChildLocks.put(transaction.getTransNum(),
                        currParent.numChildLocks.getOrDefault(transaction.getTransNum(), 0) + 1);
                currParent = currParent.parent;
            }
        }

        lockman.acquire(transaction, getResourceName(), lockType);

        // must make any necessary updates to numChildLocks
        numChildLocks.put(transaction.getTransNum(), 0);
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
        // @throws UnsupportedOperationException if context is readonly
        if (readonly) {
            throw new UnsupportedOperationException("read only.");
        }

        List<Lock> thisTransactionLocks = lockman.getLocks(transaction);
        List<Lock> descendTransactionLocks = new ArrayList<>();
        List<ResourceName> decendResourceName = new ArrayList<>();
        for (Lock l : thisTransactionLocks) {
            if (l.name.isDescendantOf(getResourceName())) {
                descendTransactionLocks.add(l);
            }
        }

        for (Lock l : descendTransactionLocks) {
            decendResourceName.add(l.name);
        }

        // check on name, if they hold a lock
        LockType currLockType = lockman.getLockType(transaction, getResourceName());
        if (currLockType.equals(LockType.NL) || currLockType == null) {
            throw new NoLockHeldException("No lock on name");
        }

        // check child lock type
        if (currLockType.equals(LockType.IS) || currLockType.equals(LockType.IX)) {
            if (numChildLocks.get(transaction.getTransNum()) != 0) {
                for (Lock l: descendTransactionLocks) {
                    if (LockType.canBeParentLock(currLockType, l.lockType)) {
                        throw new InvalidLockException("Lock can't be released, voilate" +
                                "nultigranularity");
                    }
                }
            }
        }
        lockman.release(transaction, getResourceName());

        // update child lock
        numChildLocks.put(transaction.getTransNum(), 0);

        for (ResourceName rn : decendResourceName) {
            LockContext childContext = fromResourceName(lockman, rn);
            childContext.numChildLocks.put(transaction.getTransNum(), 0);
        }

        LockContext currParent = parent;
        while (currParent != null) {

            // update level
            currParent.numChildLocks.put(transaction.getTransNum(),
                    currParent.numChildLocks.getOrDefault(transaction.getTransNum(), 0)
                    - decendResourceName.size() - 1);
            currParent = currParent.parent;
        }
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
        if (readonly) {
            throw new UnsupportedOperationException("read only.");
        }
        // check duplicate lock
        if (lockman.getLockType(transaction, getResourceName()).equals(newLockType)) {
            throw new DuplicateLockRequestException("Duplicate lock.");
        }

        // check absence lock
        if (lockman.getLockType(transaction, getResourceName()).equals(LockType.NL)
                || lockman.getLockType(transaction, getResourceName()) == null) {
            throw new NoLockHeldException("No lock for this transaction");
        }

        // invalid promote
        if (!LockType.substitutable(newLockType, lockman.getLockType(transaction, getResourceName()))) {
            if (!newLockType.equals(LockType.SIX)
                    && (!lockman.getLockType(transaction, getResourceName()).equals(LockType.IS)
                    || !lockman.getLockType(transaction, getResourceName()).equals(LockType.IX)
                    || !lockman.getLockType(transaction, getResourceName()).equals(LockType.S))
            ) {
                throw new InvalidLockException("Invalid lock promote");
            }
        }

        if (newLockType.equals(LockType.SIX)
                && ((lockman.getLockType(transaction, getResourceName()).equals(LockType.IS)
                || lockman.getLockType(transaction, getResourceName()).equals(LockType.IX)
                || lockman.getLockType(transaction, getResourceName()).equals(LockType.S)))
        ) {
            if (hasSIXAncestor(transaction)) {
                throw new InvalidLockException("Invalid lock promote");
            }
            List<ResourceName> tmp = sisDescendants(transaction);
            tmp.add(getResourceName());
//            sisDescendants(transaction).add(getResourceName());
            lockman.acquireAndRelease(transaction, getResourceName(), newLockType, tmp);

            for (ResourceName rn : sisDescendants(transaction)) {
                fromResourceName(lockman, rn).numChildLocks.put(transaction.getTransNum(), 0);
            }
            numChildLocks.put(transaction.getTransNum(),
                    numChildLocks.get(transaction.getTransNum()) - sisDescendants(transaction).size());

            LockContext currParent = parent;
            while (currParent != null) {
                currParent.numChildLocks.put(transaction.getTransNum(),
                        currParent.numChildLocks.getOrDefault(transaction.getTransNum(), 0));
                currParent = currParent.parent;
            }
            return;
        }
        lockman.promote(transaction, getResourceName(), newLockType);
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
        // @throws UnsupportedOperationException if context is readonly
        if (readonly) {
            throw new UnsupportedOperationException("read only.");
        }

        // @throws NoLockHeldException if `transaction` has no lock at this level
        if (getExplicitLockType(transaction).equals(LockType.NL)
                || getExplicitLockType(transaction) == null) {
            throw new NoLockHeldException("No lock at this level");
        }

        // if they have S or X, return
        if (getExplicitLockType(transaction).equals(LockType.S)
                || getExplicitLockType(transaction).equals(LockType.X)) {
            return;
        }

        List<ResourceName> releaseNameList = new ArrayList<>();
        List<LockType> childrenLockType = new ArrayList<>();
        List<ResourceName> childrenResourceName = new ArrayList<>();
        releaseNameList.add(getResourceName());
        childrenLockType.add(getExplicitLockType(transaction));

        List<Lock> thisTransactionLocks = lockman.getLocks(transaction);
        List<Lock> descendTransactionLocks = new ArrayList<>();
        List<ResourceName> decendResourceName = new ArrayList<>();

        for (Lock l : thisTransactionLocks) {
            if (l.name.isDescendantOf(getResourceName())) {
                descendTransactionLocks.add(l);
            }
        }

        // get the release list and children name and type list
        for (Lock l : descendTransactionLocks) {
            childrenLockType.add(l.lockType);
            childrenResourceName.add(l.name);
            releaseNameList.add(l.name);
        }
        
        LockType acquireTpye = null;
        
        // if child lock type are X/IX/SIX, set them to X. otherwise to S.
        if (childrenLockType.contains(LockType.X) || childrenLockType.contains(LockType.IX)
                || childrenLockType.contains(LockType.SIX)) {
            acquireTpye = LockType.X;
        } else {
            acquireTpye = LockType.S;
        }

        // set the type to this tree
        lockman.acquireAndRelease(transaction,getResourceName(), acquireTpye, releaseNameList);

        // update the child locks
        for (ResourceName rn : childrenResourceName) {
            fromResourceName(lockman, rn).numChildLocks.put(transaction.getTransNum(), 0);
        }
        numChildLocks.put(transaction.getTransNum(), 0);
    }


    // this method returns the type of the lock explicitly held at the current level.
    // For example, if a transaction has X(db), dbContext.getExplicitLockType(transaction)
    // should return X, but tableContext.getExplicitLockType(transaction) should return NL
    // (no lock explicitly held).
    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement  done
        return lockman.getLockType(transaction, getResourceName());
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement  done

        // Gets the type of lock that the transaction has at this level
        LockType explicitly = getExplicitLockType(transaction);

        if (parent == null) {
            return explicitly;
        }
        for (Lock l: lockman.getLocks(transaction)) {
            if (this.getResourceName().isDescendantOf(l.name)) {
                if (l.lockType.equals(LockType.X)) {
                    return l.lockType;
                }
                if (explicitly.equals(LockType.NL)) {
                    if (l.lockType.equals(LockType.SIX) || l.lockType.equals(LockType.S)) {
                        explicitly = LockType.S;
                    }
                }
            }
        }
        return explicitly;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement  done
        if (parent == null) {
            return false;
        }

        //jon's
        for (Lock l: lockman.getLocks(transaction)) {
            if (this.getResourceName().isDescendantOf(l.name) && l.lockType.equals(LockType.SIX)) {
                return true;
            }
        }
        return false;

        //cathy's
        // see if the transaction holds a SIX lock
//        return parent.getExplicitLockType(transaction).equals(LockType.SIX);
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
        if (children == null || children.isEmpty()) {
            return new ArrayList<>();
        }

        List<Lock> thisTransactionLocks = lockman.getLocks(transaction);
        List<Lock> descendTransactionLocks = new ArrayList<>();
        List<ResourceName> decendResourceName = new ArrayList<>();

        for (Lock l : thisTransactionLocks) {
            if (l.name.isDescendantOf(getResourceName())) {
                descendTransactionLocks.add(l);
            }
        }

        for (Lock l: descendTransactionLocks) {
            if (l.lockType.equals(LockType.S) || l.lockType.equals(LockType.IS)) {
                decendResourceName.add(l.name);
            }
        }
        return decendResourceName;
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

