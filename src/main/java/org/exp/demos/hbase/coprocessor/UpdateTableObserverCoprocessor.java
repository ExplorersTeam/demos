package org.exp.demos.hbase.coprocessor;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * Observation of update operations to HBase table.
 *
 * @author ChenJintong
 *
 */
public class UpdateTableObserverCoprocessor extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(UpdateTableObserverCoprocessor.class);

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        LOG.info("Start table observer coprocessor.");
        // TODO Do something.
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        LOG.info("Data has been put into table.");
        // TODO Do something.
    }

    @Override
    public boolean postCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
            ByteArrayComparable comparator, Put put, boolean result) throws IOException {
        LOG.info("Data has been checked and put into table.");
        // TODO Do something.
        return result;
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        LOG.info("Data has been deleted from table.");
        // TODO Do something.
    }

    @Override
    public boolean postCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
            ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
        LOG.info("Data has been checked and deleted from table.");
        // TODO Do something.
        return result;
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        LOG.info("Stop table observer coprocessor.");
        // Do something.
    }

}
