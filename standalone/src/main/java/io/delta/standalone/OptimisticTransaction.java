package io.delta.standalone;


import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.Protocol;
import io.delta.standalone.internal.storage.DeltaOperations;
import java.util.ConcurrentModificationException;
import java.util.List;

interface OptimisticTransaction {

    /** The version that this transaction is reading from. */
    public long readVersion();

    /** The protocol of the snapshot that this transaction is reading at. */
    public Protocol protocol();

    /**
     * Returns the metadata for this transaction. The metadata refers to the metadata of the snapshot
     * at the transaction's read version unless updated during the transaction.
     */
    public Metadata metadata();

    /**
     * Records an update to the metadata that should be committed with this transaction.
     * Note that this must be done before writing out any files so that file writing
     * and checks happen with the final metadata for the table.
     *
     * IMPORTANT: It is the responsibility of the caller to ensure that files currently
     * present in the table are still valid under the new metadata.
     */
    public void updateMetadata(Metadata metadata);

    /**
     * Records an update to the metadata that should be committed with this transaction and when
     * this transaction is logically creating a new table, e.g. replacing a previous table with new
     * metadata. Note that this must be done before writing out any files so that file writing
     * and checks happen with the final metadata for the table.
     *
     * IMPORTANT: It is the responsibility of the caller to ensure that files currently
     * present in the table are still valid under the new metadata.
     */
    public void updateMetadataForNewTable(Metadata metadata);

    /**
     * Modifies the state of the log by adding a new commit that is based on a read at
     * the given `lastVersion`.  In the case of a conflict with a concurrent writer this
     * method will throw an exception.
     *
     * @param actions Set of actions to commit
     * @param op      Details of operation that is performing this transactional commit
     */
    public long commit(List<Action> actions, DeltaOperations.Operation op) throws ConcurrentModificationException;
}

