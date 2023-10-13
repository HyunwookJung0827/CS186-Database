package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement

        // 1. Fetch the current log entry
        TransactionTableEntry logEntry = transactionTable.get(transNum);
        logEntry.transaction.setStatus(Transaction.Status.COMMITTING);

        // logManager.fetchLogRecord(logEntry.lastLSN); Usable???

        // 2. Append commit record to log
        long commitLSN = logManager.appendToLog(new CommitTransactionLogRecord(transNum, logEntry.lastLSN));

        // 3. Flush the log
        //pageFlushHook(commitLSN);
        flushToLSN(commitLSN); // which flush function?

        // 4. Update transaction table and transaction status
        logEntry.lastLSN = commitLSN;
        logEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        //transactionTable.replace(transNum, logEntry);

        // 5. return LSN of the commit record
        return commitLSN;

        //return -1L;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement

        // 1. Fetch the current log entry
        TransactionTableEntry logEntry = transactionTable.get(transNum);

        // 2. Append abort record to log
        LogRecord abortRecord = new AbortTransactionLogRecord(transNum, logEntry.lastLSN);
        long abortLSN = logManager.appendToLog(abortRecord);

        // 3. Update transaction table and transaction status
        logEntry.lastLSN = abortLSN;
        logEntry.transaction.setStatus(Transaction.Status.ABORTING);
        //transactionTable.replace(transNum, logEntry);

        // 4. return LSN of the abort record
        return abortLSN;

        // return -1L;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement

        // 1. Fetch the current log entry
        TransactionTableEntry logEntry = transactionTable.get(transNum);

        // 2. If the transaction is aborting, roll back. There are multiple records needed to roll back
        if (logEntry.transaction.getStatus().equals(Transaction.Status.ABORTING)) {
            LogRecord rec = logManager.fetchLogRecord(logEntry.lastLSN);
            while (rec != null && rec.getPrevLSN().isPresent()) {
                rec = logManager.fetchLogRecord(rec.getPrevLSN().get());
            }
            rollbackToLSN(transNum, rec.getLSN());
        }

        // 3. Append end record to log
        LogRecord endRecord = new EndTransactionLogRecord(transNum, logEntry.lastLSN);
        long endLSN = logManager.appendToLog(endRecord);

        // 4. Update transaction table and transaction status
        logEntry.lastLSN = endLSN;
        logEntry.transaction.setStatus(Transaction.Status.COMPLETE);
        transactionTable.remove(transNum);

        // 5. return LSN of the end record
        return endLSN;





//        // 1. Fetch the current log entry
//        TransactionTableEntry logEntry = transactionTable.get(transNum);
//
//        // 2. Append end record to log
//        long endLSN = logManager.appendToLog(new EndTransactionLogRecord(transNum, logEntry.lastLSN));
//
//        // 3. if the transaction is aborting, roll back
//        if (logEntry.transaction.getStatus().equals(Transaction.Status.ABORTING)) {
//            rollbackToLSN(transNum, 0);
//        }
//
//        // 4. Update transaction table and transaction status
//        logEntry.lastLSN = endLSN;
//        logEntry.transaction.setStatus(Transaction.Status.COMPLETE);
//        transactionTable.replace(transNum, logEntry);
//        // 5. return LSN of the end record
//        return endLSN;

        // return -1L;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // TODO(proj5) implement the rollback logic described above
        while (currentLSN > LSN) {
            LogRecord currRecord = logManager.fetchLogRecord(currentLSN);
            if (currRecord.isUndoable()) {
                LogRecord CLR = currRecord.undo(lastRecordLSN);
                lastRecordLSN = logManager.appendToLog(CLR);
                transactionEntry.lastLSN = lastRecordLSN;
                CLR.redo(this, diskSpaceManager, bufferManager);
            }
            currentLSN = currRecord.getUndoNextLSN().orElse(currRecord.getPrevLSN().orElse(-1L));
            //currentLSN = currRecord.getPrevLSN().orElse(-1L);
        }
        //transactionTable.get(transNum).lastLSN = lastRecordLSN;



//        long clrLSN = currentLSN;
//        while (currentLSN > LSN) {
//            LogRecord currentLogRecord = logManager.fetchLogRecord(currentLSN);
//            if (currentLogRecord.isUndoable()) {
//                // Get a compensation log record (CLR) by calling undo on the record
//                LogRecord CLR = currentLogRecord.undo(lastRecordLSN);
//
//                // Append the CLR
//                clrLSN = logManager.appendToLog(CLR);
//
//                // Call redo on the CLR to perform the undo
//                if (CLR.isRedoable()) {
//                    CLR.redo(this, diskSpaceManager, bufferManager);
//                }
//                //currentLSN = clrLSN;
//            }
//            // update the current LSN to that of the next record to undo
//            currentLSN = currentLogRecord.getPrevLSN().get();
//            // currentLSN = currentLogRecord.getUndoNextLSN().orElse(LSN);
//
//            /*else {
//                // idk
//                return;
//            }
//
//             */
//        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;

        dirtyPageTable.putIfAbsent(pageNum, LSN);
//        // I think it needs to fetch the page and modify it
//
//        byte[] page = new byte[DiskSpaceManager.PAGE_SIZE];
//        diskSpaceManager.readPage(pageNum, page);
//
//        for (int i = 0; i < after.length; i++) {
//            page[pageOffset + i] = after[i];
//        }
//
//        diskSpaceManager.writePage(pageNum, page);
//
//        // Then put it in the dirtyPageTable and transaction table
//        transactionTable.put(transNum, transactionEntry);
//        dirtyPage(pageNum, LSN);
//
//        // Flush log
        //logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);
        // TODO(proj5): implement

        rollbackToLSN(transNum, savepointLSN);
        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table

        // 1. iterate through the dirtyPageTable
        int dirtySize = dirtyPageTable.size();
        int transSize = transactionTable.size();
        Iterator<Map.Entry<Long, Long>> dirtyIter = dirtyPageTable.entrySet().iterator();
        Iterator<Map.Entry<Long, TransactionTableEntry>> transIter = transactionTable.entrySet().iterator();

        while (dirtyIter.hasNext()) {
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size() + 1, chkptTxnTable.size())) {
                // append to log
                EndCheckpointLogRecord endRecord = new EndCheckpointLogRecord(new HashMap<>(chkptDPT), new HashMap<>(chkptTxnTable));
                logManager.appendToLog(endRecord);
                flushToLSN(endRecord.getLSN());
                chkptDPT.clear();
            }

            // Add next entry to chkptDPT
            Map.Entry<Long, Long> nextDirtyEntry = dirtyIter.next();
            chkptDPT.put(nextDirtyEntry.getKey(), nextDirtyEntry.getValue());
        }

        while (transIter.hasNext()) {
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size() + 1)) {
                // append to log
                EndCheckpointLogRecord endRecord = new EndCheckpointLogRecord(new HashMap<>(chkptDPT), new HashMap<>(chkptTxnTable));
                logManager.appendToLog(endRecord);
                flushToLSN(endRecord.getLSN());

                chkptDPT.clear();
                chkptTxnTable.clear();
            }

            // Add next entry to chkptDPT
            Map.Entry<Long, TransactionTableEntry> nextTransEntry = transIter.next();
            Pair<Transaction.Status, Long> value = new Pair<>(nextTransEntry.getValue().transaction.getStatus(), nextTransEntry.getValue().lastLSN);
            chkptTxnTable.put(nextTransEntry.getKey(), value);
        }
/*
        for (Map.Entry<Long, Long> dirtyPageEntry: dirtyPageTable.entrySet()) {

            if (EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size()) {
                // append to log
                EndCheckpointLogRecord endChk = new EndCheckpointLogRecord(new HashMap<>(chkptDPT), new HashMap<>(chkptTxnTable));
                logManager.appendToLog(endChk);
                flushToLSN(endChk.getLSN());

                chkptDPT.clear();

            }
        }

 */



        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(endRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement

        Iterator<LogRecord> lsnIter = logManager.scanFrom(LSN);

        while (lsnIter.hasNext()) {
            LogRecord nextRecord = lsnIter.next();
            LogType nextRecordType = nextRecord.getType();
            //Map<Long, Long> chkptDPT = nextRecord.getDirtyPageTable();

            // 2-1. If the log record is for a transaction operation (getTransNum is present)
            // - update the transaction table
            if (nextRecord.getTransNum().isPresent()) {
                Long transNum = nextRecord.getTransNum().get();
                if (transactionTable.get(transNum) == null) {
                    Transaction trans = newTransaction.apply(transNum);
                    startTransaction(trans);
                }
                TransactionTableEntry entry = transactionTable.get(transNum);
                entry.lastLSN = nextRecord.getLSN();
                // temp fix
                transactionTable.replace(transNum, entry);
            }

            // 2-2. If the log record is page-related (getPageNum is present), update the dpt
            //     *   - update/undoupdate page will dirty pages
            //     *   - free/undoalloc page always flush changes to disk
            //     *   - no action needed for alloc/undofree page
            if (nextRecord.getPageNum().isPresent()) {
                if (nextRecord.getType().equals(LogType.UPDATE_PAGE) || nextRecord.getType().equals(LogType.UNDO_UPDATE_PAGE)) {
                    dirtyPage(nextRecord.getPageNum().get(), nextRecord.getLSN());
                }
                if (nextRecord.getType().equals(LogType.FREE_PAGE) || nextRecord.getType().equals(LogType.UNDO_ALLOC_PAGE)) {
                    Long recPageNum = nextRecord.getPageNum().get();
                    dirtyPageTable.remove(recPageNum);
                    // temp fix
                    flushToLSN(nextRecord.getLSN());
                }
            }

            // 2-3. If the log record is for a change in transaction status:
            // * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
            //     * - update the transaction table
            //     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
            //     *   from txn table, and add to endedTransactions
            //TransactionTableEntry logEntry = transactionTable.get(transNum);
            if (nextRecord.getType().equals(LogType.COMMIT_TRANSACTION)) {
                Long transNum = nextRecord.getTransNum().get();
                TransactionTableEntry logEntry = transactionTable.get(transNum);
                logEntry.transaction.setStatus(Transaction.Status.COMMITTING);
                transactionTable.replace(transNum, logEntry);
            }
            else if (nextRecord.getType().equals(LogType.ABORT_TRANSACTION)) {
                Long transNum = nextRecord.getTransNum().get();
                TransactionTableEntry logEntry = transactionTable.get(transNum);
                logEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                transactionTable.replace(transNum, logEntry);
            }
            else if (nextRecord.getType().equals(LogType.END_TRANSACTION)) {
                Long transNum = nextRecord.getTransNum().get();
                TransactionTableEntry logEntry = transactionTable.get(transNum);
                // The transaction should also be cleaned up before setting the status
                // temp fix
                logEntry.transaction.cleanup();


                // Set up the status
                logEntry.transaction.setStatus(Transaction.Status.COMPLETE);

                // The entry should be removed from the transaction table

                transactionTable.remove(transNum);

                // Additionally, you should add the ended transaction's transaction number into the endedTransactions set,
                // which will be important for processing end checkpoint records.
                endedTransactions.add(transNum);
            }

            // 2-4. If the log record is an end_checkpoint record:
            //if (nextRecord.getType().equals(EndCheckpointLogRecord.class)) {
            else if (nextRecord.getType().equals(LogType.END_CHECKPOINT)) {
                //     * - Copy all entries of checkpoint DPT (replace existing entries if any)
                Map<Long, Long> chkptDPT = nextRecord.getDirtyPageTable();
                Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = nextRecord.getTransactionTable();
                dirtyPageTable.clear();
                dirtyPageTable.putAll(chkptDPT);





                for (Map.Entry<Long, Pair<Transaction.Status, Long>> txnEntry: chkptTxnTable.entrySet()) {
                    long nextTransNumber = txnEntry.getKey();
                    if (!endedTransactions.contains(nextTransNumber)) {
                        //     * - Skip txn table entries for transactions that have already ended
                        //     * - Add to transaction table if not already present
                        if (!transactionTable.containsKey(nextTransNumber)) {
                            startTransaction(newTransaction.apply(nextTransNumber));
                        }

                        //     * - Update lastLSN to be the larger of the existing entry's (if any) and
                        //     *   the checkpoint's
                        TransactionTableEntry transTblEntry = transactionTable.get(nextTransNumber);
                        if (txnEntry.getValue().getSecond() >= transTblEntry.lastLSN) {
                            transTblEntry.lastLSN = txnEntry.getValue().getSecond();
                            transactionTable.replace(nextTransNumber, transTblEntry);
                        }

                        //     * - The status's in the transaction table should be updated if it is possible
                        //     *   to transition from the status in the table to the status in the
                        //     *   checkpoint. For example, running -> aborting is a possible transition,
                        //     *   but aborting -> running is not.
                        Transaction.Status chkptTxnStatus = txnEntry.getValue().getFirst();
                        Transaction.Status txnTableStatus = transTblEntry.transaction.getStatus();
                        if (toInt(chkptTxnStatus) >= toInt(txnTableStatus)) {
                            // Update txnEntry if Checkpoint won
                            if (chkptTxnStatus.equals(Transaction.Status.ABORTING)) {
                                chkptTxnStatus = Transaction.Status.RECOVERY_ABORTING;
                            }
                            transTblEntry.transaction.setStatus(chkptTxnStatus);
                            transactionTable.replace(nextTransNumber, transTblEntry);
                        }
                    }
                }
            }

        }
        // * After all records in the log are processed, for each ttable entry:
        for (Map.Entry<Long, TransactionTableEntry> tPair : transactionTable.entrySet()) {
            Long txnNum = tPair.getKey();
            TransactionTableEntry txnEntry = tPair.getValue();
            Transaction txn = txnEntry.transaction;
            Transaction.Status txnStatus = txnEntry.transaction.getStatus();

            //     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
            //     *    remove from the ttable, and append an end record
            if (txnStatus.equals(Transaction.Status.COMMITTING)) {
                txn.cleanup();
                end(txnNum);
                //txnEntry.setStatus(Transaction.Status.COMPLETE);
            }

            //     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
            //     *    record
            if (txnStatus.equals(Transaction.Status.RUNNING)) {
                abort(txnNum);
                txn.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }

            //     *  - if RECOVERY_ABORTING: no action needed
        }
        return;
    }

    /**
     * Does boolean checks to see what kind of transaction a status is
     *
     * @param s
     * @return Number denoting status of the transaction
     */
    public int toInt(Transaction.Status s) {
        if (s.equals(Transaction.Status.RUNNING)) return 0;
        if (s.equals(Transaction.Status.ABORTING) || s.equals(Transaction.Status.COMMITTING)) return 1;
        if (s.equals(Transaction.Status.COMPLETE)) return 2;
        return 3;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // TODO(proj5): implement
        // Check DPT for starting point
        Long lowestRecLSN = Collections.min(dirtyPageTable.values());
        Iterator<LogRecord> logIter = logManager.scanFrom(lowestRecLSN);
        while (logIter.hasNext()) {
            LogRecord nextRecord = logIter.next();
            if (!nextRecord.isRedoable()) {
                continue;
            } else {
                LogType recordType = nextRecord.getType();

                // MAKE SURE IT's ALL PART, NOT PAGE
                if (recordType.equals(LogType.ALLOC_PART) || recordType.equals(LogType.FREE_PART) || recordType.equals(LogType.UNDO_ALLOC_PART) || recordType.equals(LogType.UNDO_FREE_PART)) {
                    nextRecord.redo(this, diskSpaceManager, bufferManager);
                }
                else if (recordType.equals(LogType.ALLOC_PAGE) || recordType.equals(LogType.UNDO_FREE_PAGE)) {
                    nextRecord.redo(this, diskSpaceManager, bufferManager);
                }
                else if (recordType.equals(LogType.UPDATE_PAGE) || recordType.equals(LogType.UNDO_UPDATE_PAGE) || recordType.equals(LogType.FREE_PAGE) || recordType.equals(LogType.UNDO_ALLOC_PAGE)) {
                    Boolean terminate = Boolean.FALSE;
                    if (nextRecord.getPageNum().isPresent()) {
                        Long pageNum = nextRecord.getPageNum().get();
                        if (dirtyPageTable.containsKey(pageNum) && (nextRecord.getLSN() >= dirtyPageTable.get(pageNum))) {
                            Page page = bufferManager.fetchPage(new DummyLockContext(), pageNum);
                            try {
                                // Do anything that requires the page here
                                if (nextRecord.getLSN() > page.getPageLSN()) {
                                    nextRecord.redo(this, diskSpaceManager, bufferManager);
                                }
                            } finally {
                                page.unpin();
                            }
                        }
                    }
                }
            }
        }


        return;
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   or the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        PriorityQueue<Pair<Long, Long>> prQ = new PriorityQueue<>(new PairFirstReverseComparator<Long, Long>());
        for (Map.Entry<Long, TransactionTableEntry> tPair : transactionTable.entrySet()) {
            TransactionTableEntry tEntry = tPair.getValue();
            if (tEntry.transaction.getStatus().equals(Transaction.Status.RECOVERY_ABORTING)) {
                // *temp fix*
                prQ.add(new Pair<Long, Long>(tEntry.lastLSN, tPair.getKey()));
                //prQ.add(new Pair<Long, Long>(tPair.getKey(), tEntry.lastLSN));
            }
        }
        // priorityQueue Finished
/*

        Map<Long, Long> lastLSNTable = new HashMap<>();


        while (!prQ.isEmpty()) {
            // qEntry: (lastLSN, transaction Number)
            Pair<Long, Long> qEntry = prQ.poll();
            Long qTransNum = qEntry.getSecond();

            TransactionTableEntry transEntry = transactionTable.get(qTransNum);
            // temp fix
             Long qLastLSN = qEntry.getFirst();
            //Long qLastLSN = transEntry.lastLSN;
            LogRecord qRecord = logManager.fetchLogRecord(qLastLSN);

            // - 1. if the record is undoable, undo it, and append the appropriate CLR
            // The undo method of LogRecord does not actually undo changes
            // - it instead returns the compensation log record.
            // To actually undo changes,
            // you will need to append the returned CLR and then call redo on it.
            if (qRecord.isUndoable()) {
                LogRecord CLR = qRecord.undo(qLastLSN);
                //transEntry.lastLSN = logManager.appendToLog(CLR);
                logManager.appendToLog(CLR);
                CLR.redo(this, diskSpaceManager, bufferManager);
                //transactionTable.replace(transEntry.transaction.getTransNum(), transEntry);
            }


            // * - 2. replace the entry with a new one, using the undoNextLSN if available,
            // *   or the prevLSN otherwise.
            Long nextLSN = 0L;
            if (qRecord.getUndoNextLSN().isPresent()) { // might need to be else if
                nextLSN = qRecord.getUndoNextLSN().get();
            } else {
                // *temp fix*
                //nextLSN = qRecord.getPrevLSN().get();
                if (qRecord.getPrevLSN().isPresent()) { // this set the lsn of t2 from 20134 to 20039
                    nextLSN = qRecord.getPrevLSN().get();
                }
            }
            // *temp fix*
            // prQ.add(new Pair<>(nextLSN, qTransNum));
            transEntry.lastLSN = nextLSN;
            transactionTable.replace(qTransNum, transEntry);

            // * - 3. if the new LSN is 0, clean up the transaction, set the status to complete,
            // *   and remove from transaction table.
            if (nextLSN == 0) {
                transEntry.transaction.cleanup();
                end(qTransNum);
            } else {
                //prQ.add(new Pair<>(qTransNum, nextLSN));
                prQ.add(new Pair<>(nextLSN, qTransNum));
            }

        } // while loop ended
*/














        Map<Long, Long> lastLSNTable = new HashMap<>();


        while (!prQ.isEmpty()) {
//            Pair<Long, Long> qEntry = prQ.poll();
//            Long qTransNum = qEntry.getSecond();
//            Long qLastLSN = transactionTable.get(qTransNum).lastLSN;
//            //if (lastLSNTable.containsKey(qTransNum)) {
//            if (lastLSNTable.containsKey(qTransNum)) {
//                if (qLastLSN < lastLSNTable.get(qTransNum)) {
//                    lastLSNTable.replace(qTransNum, qLastLSN);
//                }
//            }
//            else lastLSNTable.put(qTransNum, qLastLSN);
//
//            //Long qLastLSN = qEntry.getFirst();
//            LogRecord qRecord = logManager.fetchLogRecord(qLastLSN);
//            TransactionTableEntry transEntry = transactionTable.get(qTransNum);

            Pair<Long, Long> qEntry = prQ.poll();
            Long qTransNum = qEntry.getSecond();
            Long qLastLSN = qEntry.getFirst();
            LogRecord qRecord = logManager.fetchLogRecord(qLastLSN);
            TransactionTableEntry transEntry = transactionTable.get(qTransNum);


            // - 1. if the record is undoable, undo it, and append the appropriate CLR
            // The undo method of LogRecord does not actually undo changes
            // - it instead returns the compensation log record.
            // To actually undo changes,
            // you will need to append the returned CLR and then call redo on it.
            if (qRecord.isUndoable()) {
                LogRecord CLR = qRecord.undo(transEntry.lastLSN);
                // *temp fix*
                //logManager.appendToLog(CLR);
                transEntry.lastLSN = logManager.appendToLog(CLR);
                CLR.redo(this, diskSpaceManager, bufferManager);
            }

            // * - 2. replace the entry with a new one, using the undoNextLSN if available,
            // *   or the prevLSN otherwise.
            Long nextLSN = 0L;
            if (qRecord.getUndoNextLSN().isPresent()) { // might need to be else if
                nextLSN = qRecord.getUndoNextLSN().get();
            } else {
                // *temp fix*
                //nextLSN = qRecord.getPrevLSN().get();
                if (qRecord.getPrevLSN().isPresent()) { // this set the lsn of t2 from 20134 to 20039
                    nextLSN = qRecord.getPrevLSN().get();
                }
            }
            // *temp fix*
            //prQ.add(new Pair<>(nextLSN, qTransNum));
//            transEntry.lastLSN = nextLSN;
//            transactionTable.replace(qTransNum, transEntry);


            // * - 3. if the new LSN is 0, clean up the transaction, set the status to complete,
            // *   and remove from transaction table.
            if (nextLSN == 0) {
                transEntry.transaction.cleanup();
                end(qTransNum);
            } else {
                //prQ.add(new Pair<>(qTransNum, nextLSN));
                prQ.add(new Pair<>(nextLSN, qTransNum));
            }
        }

        return;
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
