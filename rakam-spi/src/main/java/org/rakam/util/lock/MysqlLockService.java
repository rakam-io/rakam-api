package org.rakam.util.lock;

import com.google.common.base.Throwables;
import org.rakam.analysis.JDBCPoolDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.BooleanMapper;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class MysqlLockService
        implements LockService {
    private final DBI dbi;
    private Handle currentHandle;
    private Set<String> locks;

    public MysqlLockService(JDBCPoolDataSource poolDataSource) {
        this.dbi = new DBI(() -> {
            return poolDataSource.getConnection(true);
        });
        this.currentHandle = dbi.open();
        locks = new ConcurrentSkipListSet<>();
    }

    @Override
    public Lock tryLock(String name) {
        if (!locks.add(name)) {
            return null;
        }
        try {
            return tryLock(name, 4);
        } catch (Exception e) {
            locks.remove(name);
            throw Throwables.propagate(e);
        }
    }

    public Lock tryLock(String name, int tryCount) {
        Boolean first;
        try {
            first = currentHandle.createQuery("select get_lock(:name, 1)")
                    .bind("name", name)
                    .map(BooleanMapper.FIRST)
                    .first();
        } catch (Exception e) {
            synchronized (this) {
                currentHandle = dbi.open();
            }

            if (tryCount == 0) {
                throw Throwables.propagate(e);
            }

            return tryLock(name, tryCount - 1);
        }

        if (!Boolean.TRUE.equals(first)) {
            locks.remove(name);
            return null;
        }

        return () -> {
            locks.remove(name);
            currentHandle.createQuery("select release_lock(:name)")
                    .bind("name", name)
                    .map(BooleanMapper.FIRST)
                    .first();
        };
    }
}
