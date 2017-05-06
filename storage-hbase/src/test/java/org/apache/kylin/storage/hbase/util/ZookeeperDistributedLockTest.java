/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.storage.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.lock.DistributedLock.Watcher;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZookeeperDistributedLockTest extends HBaseMetadataTestCase {

    static ZookeeperDistributedLock.Factory factory;

    static String rand() {
        return "" + new Random().nextInt(10000000);
    }

    @BeforeClass
    public static void setup() throws Exception {
        staticCreateTestMetadata();
        factory = new ZookeeperDistributedLock.Factory();
    }

    @AfterClass
    public static void after() throws Exception {
        staticCleanupTestMetadata();
    }

    @Test
    public void testBasic() {
        DistributedLock l = factory.lockForCurrentThread();
        String path = "/kylin/test/ZookeeperDistributedLockTest/testBasic/" + rand();

        assertTrue(l.isLocked(path) == false);
        assertTrue(l.lock(path));
        assertTrue(l.lock(path));
        assertTrue(l.lock(path));
        assertEquals(l.getClient(), l.peekLock(path));
        l.unlock(path);
        assertTrue(l.isLocked(path) == false);
    }

    @Test
    public void testErrorCases() {
        DistributedLock c = factory.lockForClient("client1");
        DistributedLock d = factory.lockForClient("client2");
        String path = "/kylin/test/ZookeeperDistributedLockTest/testErrorCases/" + rand();

        assertTrue(c.isLocked(path) == false);
        assertTrue(d.peekLock(path) == null);

        assertTrue(c.lock(path));
        assertTrue(d.lock(path) == false);
        assertTrue(d.isLocked(path) == true);
        assertEquals(c.getClient(), d.peekLock(path));

        try {
            d.unlock(path);
            fail();
        } catch (IllegalStateException ex) {
            // expected
        }

        c.unlock(path);
        assertTrue(d.isLocked(path) == false);

        d.lock(path);
        d.unlock(path);
    }

    @Test
    public void testLockTimeout() throws InterruptedException {
        final DistributedLock c = factory.lockForClient("client1");
        final DistributedLock d = factory.lockForClient("client2");
        final String path = "/kylin/test/ZookeeperDistributedLockTest/testLockTimeout/" + rand();

        assertTrue(c.isLocked(path) == false);
        assertTrue(d.peekLock(path) == null);

        assertTrue(c.lock(path));
        new Thread() {
            @Override
            public void run() {
                d.lock(path, 10000);
            }
        }.start();
        c.unlock(path);

        Thread.sleep(10000);

        assertTrue(c.isLocked(path));
        assertEquals(d.getClient(), d.peekLock(path));
        d.unlock(path);
    }

    @Test
    public void testWatch() throws InterruptedException, IOException {
        // init lock paths
        final String base = "/kylin/test/ZookeeperDistributedLockTest/testWatch/" + rand();
        final int nLocks = 3;
        final String[] lockPaths = new String[nLocks];
        for (int i = 0; i < nLocks; i++)
            lockPaths[i] = base + "/" + (i + 1);

        // init clients
        final int[] clientIds = new int[] { 2, 3, 5, 7, 11, 13, 17, 19, 23, 29 };
        final int nClients = clientIds.length;
        final DistributedLock[] clients = new DistributedLock[nClients];
        for (int i = 0; i < nClients; i++) {
            clients[i] = factory.lockForClient("" + clientIds[i]);
        }

        // init watch
        DistributedLock lock = factory.lockForClient("");
        final AtomicInteger countSum = new AtomicInteger(0);
        final AtomicInteger scoreSum = new AtomicInteger(0);
        Closeable watch = lock.watchLocks(base, Executors.newFixedThreadPool(1), new Watcher() {

            @Override
            public void onLock(String lockPath, String client) {
                countSum.incrementAndGet();
                int cut = lockPath.lastIndexOf("/");
                int lockId = Integer.parseInt(lockPath.substring(cut + 1));
                int clientId = Integer.parseInt(client);
                scoreSum.addAndGet(lockId * clientId);
            }

            @Override
            public void onUnlock(String lockPath, String client) {
                countSum.decrementAndGet();
            }
        });

        // init client threads
        ClientThread[] threads = new ClientThread[nClients];
        for (int i = 0; i < nClients; i++) {
            DistributedLock client = clients[i];
            threads[i] = new ClientThread(client, lockPaths);
            threads[i].start();
        }

        // wait done
        for (int i = 0; i < nClients; i++) {
            threads[i].join();
        }
        
        // verify sum
        assertEquals(0, countSum.get());
        int expectedScore = 0;
        for (int i = 0; i < nClients; i++) {
            expectedScore += threads[i].counter * clientIds[i];
        }
        assertEquals(expectedScore, scoreSum.get());
        watch.close();

        // assert all locks were released
        for (int i = 0; i < nLocks; i++) {
            assertTrue(lock.isLocked(lockPaths[i]) == false);
        }
    }

    class ClientThread extends Thread {
        DistributedLock client;
        String[] lockPaths;
        int nLocks;
        int counter = 0;

        ClientThread(DistributedLock client, String[] lockPaths) {
            this.client = client;
            this.lockPaths = lockPaths;
            this.nLocks = lockPaths.length;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            Random rand = new Random();

            while (System.currentTimeMillis() - start <= 15000) {
                try {
                    Thread.sleep(rand.nextInt(200));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // random lock
                int lockIdx = rand.nextInt(nLocks);
                boolean locked = client.lock(lockPaths[lockIdx]);
                if (locked)
                    counter += (lockIdx + 1);

                // random unlock
                try {
                    client.unlock(lockPaths[lockIdx]);
                } catch (IllegalStateException e) {
                    // ignore
                }
            }

            // clean up, unlock all
            for (String lockPath : lockPaths) {
                try {
                    client.unlock(lockPath);
                } catch (IllegalStateException e) {
                    // ignore
                }
            }
        }
    };
}
