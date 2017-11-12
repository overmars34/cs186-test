import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hashtable that is keyed on the name of the
 * table being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {
    private DeadlockAvoidanceType deadlockAvoidanceType;
    private HashMap<String, TableLock> tableToTableLock;

    public enum DeadlockAvoidanceType {
        None,
        WaitDie,
        WoundWait
    }

    public enum LockType {
        Shared,
        Exclusive
    }

    public LockManager(DeadlockAvoidanceType type) {
        this.deadlockAvoidanceType = type;
        this.tableToTableLock = new HashMap<>();
    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue. Once you have implemented deadlock avoidance algorithms, you
     * should instead check the deadlock avoidance type and call the
     * appropriate function that you will complete in part 2.
     * @param transaction that is requesting the lock
     * @param tableName of requested table
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, String tableName, LockType lockType)
            throws IllegalArgumentException {

        if (transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException("Can't acquire from waiting TX");
        }

        if (tableToTableLock.containsKey(tableName)) {
            TableLock lock = tableToTableLock.get(tableName);

            if (lock.lockOwners.contains(transaction)
                && lock.lockType == LockType.Exclusive
                && lockType == LockType.Shared) {
                throw new IllegalArgumentException("Can't get S already have X");
            }

            if (lock.lockOwners.contains(transaction)
                && lock.lockType == lockType) {
                throw new IllegalArgumentException("Can't re-request same lock");
            }

            // T is the only one holding a shared lock and is now requested
            // and exclusive one so just update the lock type to X
            if (lock.lockOwners.contains(transaction)
                && lock.lockOwners.size() == 1
                && lock.lockType == LockType.Shared
                && lockType == LockType.Exclusive) {
                lock.lockType = LockType.Exclusive;
                return;
            }

            if (lock.lockType.equals(LockType.Exclusive)) {

                // DL Avoidance
                if (deadlockAvoidanceType.equals(DeadlockAvoidanceType.WaitDie)) {
                    waitDie(tableName, transaction, lockType);
                } else {
                    woundWait(tableName, transaction, lockType);
                }
            } else {
                if (lockType.equals(LockType.Shared)) {
                    lock.lockOwners.add(transaction);
                } else {

                    // DL Avoidance
                    if (deadlockAvoidanceType.equals(DeadlockAvoidanceType.WaitDie)) {
                        waitDie(tableName, transaction, lockType);
                    } else {
                        woundWait(tableName, transaction, lockType);
                    }
                }
            }
        } else {
            TableLock lock = new TableLock(lockType);
            lock.lockOwners.add(transaction);
            tableToTableLock.put(tableName, lock);
        }
    }

    /**
     * This method will return true if the requested lock is compatible. See
     * spec provides compatibility conditions.
     * @param tableName of requested table
     * @param transaction requesting the lock
     * @param lockType of the requested lock
     * @return true if the lock being requested does not cause a conflict
     */
    private boolean compatible(String tableName, Transaction transaction, LockType lockType) {
        return false; // Not really needed so ignoring.
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param tableName of table being released
     */
    public void release(Transaction transaction, String tableName) throws IllegalArgumentException{
        if (transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException("Can't release from Waiting TX");
        }

        TableLock lock = tableToTableLock.getOrDefault(tableName, null);
        if (lock == null) {
            throw new IllegalArgumentException("Lock not held by TX");
        }

        // Release the lock. It might be re-acquired later as an X
        lock.lockOwners.remove(transaction);

        if (lock.lockOwners.size() > 1) {
            return;
        } else if (lock.lockOwners.isEmpty()) {
            tableToTableLock.remove(tableName);
        }

        // No-one is waiting for this table. Just release and return early.
        if (lock.requestersQueue.size() == 0) {
            return;
        }

        // Look for promotion of current T
        if (lock.lockType.equals(LockType.Shared)) {
            lock.requestersQueue
                .stream()
                .filter(x -> x.transaction.equals(transaction))
                .filter(x -> x.lockType.equals(LockType.Exclusive))
                .findFirst()
                .ifPresent(r -> {
                    TableLock newLock = new TableLock(LockType.Exclusive);
                    newLock.lockOwners.add(transaction);
                    newLock.requestersQueue = lock.requestersQueue;
                    lock.requestersQueue.remove(r);

                    tableToTableLock.remove(tableName);
                    tableToTableLock.put(tableName, newLock);
                });
        }

        // Give priority to anyone else trying to promote a lock, then to
        // anyone trying to get an X.
        Optional<Request> r =
            lock.requestersQueue
                .stream()
                .filter(x -> x.lockType.equals(LockType.Exclusive)
                    && lock.lockOwners.contains(x.transaction))
                .findFirst();
        if (!r.isPresent()) {
            r = lock.requestersQueue
                    .stream()
                    .filter(x -> x.lockType.equals(LockType.Exclusive))
                    .findFirst();
        }

        // Able to grant exactly ONE exclusive lock
        if (r.isPresent()) {
            lock.requestersQueue.remove(r.get());

            TableLock newLock = new TableLock(r.get().lockType);
            newLock.requestersQueue = lock.requestersQueue;
            newLock.lockOwners.add(r.get().transaction);
            r.get().transaction.setStatus(Transaction.Status.Running);

            tableToTableLock.put(tableName, newLock);
        } else {
            // No one on the wait queue was asking for an exclusive so
            // granting SHARED to everyone.
            TableLock newLock = new TableLock(LockType.Shared);
            lock.requestersQueue
                .forEach(lo -> {
                    newLock.lockOwners.add(lo.transaction);
                    lo.transaction.setStatus(Transaction.Status.Running);
                });

            tableToTableLock.put(tableName, newLock);
        }
    }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the table tableName.
     * @param transaction holding lock
     * @param tableName of locked table
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, String tableName, LockType lockType) {
        if (tableToTableLock.containsKey(tableName)) {
            TableLock lock = tableToTableLock.get(tableName);
            return lock.lockOwners.contains(transaction)
                && lock.lockType.equals(lockType);
        } else {
            return false;
        }
    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will abort if it has
     * a lower priority (higher timestamp) than all conflicting transactions.
     * If t1 has a higher priority, it will wait on the requesters queue.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void waitDie(String tableName, Transaction transaction, LockType lockType) {
        TableLock lock = tableToTableLock.get(tableName);
        int prior = transaction.getTimestamp();

        if (lock.lockOwners.stream()
                           .allMatch(x -> x.getTimestamp() > prior)) {
            // All transactions have newer timestamps than prior so
            // that means prior is higher priority them all of them.
            // So we wait...
            tableToTableLock.get(tableName)
                            .requestersQueue
                            .add(new Request(transaction, lockType));
            transaction.sleep();
        } else {
            // The transaction is of lower priority than the lock
            // holders so it should die in :fire:
            transaction.abort();
        }
    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will wait if it has
     * a lower priority (higher timestamp) than conflicting transactions. If t1
     * has a higher priority than every conflicting transaction, it will abort
     * all the lock holders and acquire the lock.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void woundWait(String tableName, Transaction transaction, LockType lockType) {
        TableLock lock = tableToTableLock.get(tableName);
        int prior = transaction.getTimestamp();

        if (lock.lockOwners.stream().allMatch(x -> x.getTimestamp() > prior)) {

            // All lock holders have lower priority...kill them
            lock.lockOwners.forEach(x -> {
                x.abort();

                for (Request r : (LinkedList<Request>)lock.requestersQueue.clone()) {
                    if (r.transaction.equals(x)) {
                        lock.requestersQueue.remove(r);
                    }
                }
            });

            lock.lockOwners.clear();

            // Let this transaction acquire the lock ...
            lock.lockOwners.add(transaction);
            lock.lockType = lockType;
        } else {
            // Need to just wait as usual

            tableToTableLock.get(tableName)
                            .requestersQueue
                            .add(new Request(transaction, lockType));
            transaction.sleep();
        }
    }

    /**
     * Contains all information about the lock for a specific table. This
     * information includes lock type, lock owner(s), and lock requestor(s).
     */
    private class TableLock {
        private LockType lockType;
        private HashSet<Transaction> lockOwners;
        private LinkedList<Request> requestersQueue;

        public TableLock(LockType lockType) {
            this.lockType = lockType;
            this.lockOwners = new HashSet<>();
            this.requestersQueue = new LinkedList<>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requestor queue for a specific table
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }
    }
}
