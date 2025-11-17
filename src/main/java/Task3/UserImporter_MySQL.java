package Task3;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class UserImporter_MySQL {
    private static final int EXPECTED_COLUMNS = 8;
    private static final int BATCH_SIZE = 1000;
    private static final int THREAD_COUNT = 6;
    // 修改为 MySQL 连接配置
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/project_db?useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
    private static final String JDBC_USER = "root";
    private static final String JDBC_PASS = "1108"; // 修改为你的 MySQL 密码

    // 简单连接池
    private static final BlockingQueue<Connection> connectionPool = new LinkedBlockingQueue<>();
    private static boolean poolInitialized = false;

    // 初始化连接池
    private static void initializeConnectionPool() throws SQLException {
        if (poolInitialized) return;

        System.out.println("初始化 MySQL 连接池，大小: " + THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
            conn.setAutoCommit(false);  // 统一设置不自动提交
            connectionPool.offer(conn);
        }
        poolInitialized = true;
    }

    // 从连接池获取连接
    private static Connection getConnectionFromPool() throws InterruptedException {
        return connectionPool.take();
    }

    // 归还连接到连接池
    private static void returnConnectionToPool(Connection conn) {
        if (conn != null) {
            try {
                // 确保连接处于可用状态
                if (conn.isClosed()) {
                    // 如果连接已关闭，创建新连接替代
                    Connection newConn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
                    newConn.setAutoCommit(false);
                    connectionPool.offer(newConn);
                } else {
                    // 回滚任何未提交的事务，确保连接干净
                    try { conn.rollback(); } catch (SQLException e) { /* 忽略回滚错误 */ }
                    connectionPool.offer(conn);
                }
            } catch (SQLException e) {
                System.err.println("归还连接失败: " + e.getMessage());
            }
        }
    }

    // 关闭连接池
    private static void closeConnectionPool() {
        while (!connectionPool.isEmpty()) {
            Connection conn = connectionPool.poll();
            if (conn != null) {
                try { conn.close(); } catch (SQLException e) { /* 忽略关闭错误 */ }
            }
        }
        poolInitialized = false;
    }

    public static void createUserTable() throws SQLException, InterruptedException {
        // 使用连接池中的连接创建表
        initializeConnectionPool();
        Connection conn = null;

        try {
            conn = getConnectionFromPool();

            try (Statement stmt = conn.createStatement()) {
                // 修改为 MySQL 语法
                String sql = "CREATE TABLE IF NOT EXISTS users (" +
                        "author_id INT PRIMARY KEY NOT NULL, " +
                        "author_name VARCHAR(100) NOT NULL, " +
                        "gender VARCHAR(10), " +  // MySQL 不支持 CHECK 约束
                        "age INT, " +             // MySQL 不支持 CHECK 约束
                        "followers_count INT DEFAULT 0, " +
                        "following_count INT DEFAULT 0, " +
                        "user_followers TEXT, " +
                        "user_following TEXT" +
                        ") ENGINE=InnoDB";        // 指定存储引擎
                stmt.execute(sql);
                System.out.println("MySQL users 表创建完成");
            }

            try (Statement stmt = conn.createStatement()) {
                // 修改为 MySQL 语法
                String sql = "CREATE TABLE IF NOT EXISTS user_followers (" +
                        "id INT AUTO_INCREMENT PRIMARY KEY, " +  // MySQL 自增语法
                        "user_id INT NOT NULL, " +
                        "follower_id INT NOT NULL, " +
                        "FOREIGN KEY (user_id) REFERENCES users(author_id)," +
                        "FOREIGN KEY (follower_id) REFERENCES users(author_id), " +
                        "UNIQUE KEY unique_user_follower (user_id, follower_id)" +  // MySQL 唯一约束语法
                        ") ENGINE=InnoDB";
                stmt.execute(sql);
                System.out.println("MySQL user_followers 表创建完成");
            }

            try (Statement stmt = conn.createStatement()) {
                // 修改为 MySQL 语法
                String sql = "CREATE TABLE IF NOT EXISTS user_following (" +
                        "id INT AUTO_INCREMENT PRIMARY KEY, " +
                        "user_id INT NOT NULL, " +
                        "following_id INT NOT NULL, " +
                        "FOREIGN KEY (user_id) REFERENCES users(author_id)," +
                        "FOREIGN KEY (following_id) REFERENCES users(author_id), " +
                        "UNIQUE KEY unique_user_following (user_id, following_id)" +
                        ") ENGINE=InnoDB";
                stmt.execute(sql);
                System.out.println("MySQL user_following 表创建完成");
            }

            conn.commit();  // 提交DDL操作

        } catch (Exception e) {
            if (conn != null) {
                try { conn.rollback(); } catch (SQLException ex) { /* 忽略回滚错误 */ }
            }
            throw new SQLException("创建表失败: " + e.getMessage(), e);
        } finally {
            returnConnectionToPool(conn);
        }
    }

    public static void importUserData(List<String[]> rows) throws Exception {
        // 初始化连接池
        initializeConnectionPool();

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger usersInserted = new AtomicInteger(0);
        AtomicInteger skipped = new AtomicInteger(0);
        long totalStartTime = System.currentTimeMillis();

        List<List<String[]>> partitions = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += BATCH_SIZE) {
            partitions.add(rows.subList(i, Math.min(i + BATCH_SIZE, rows.size())));
        }

        CountDownLatch latch = new CountDownLatch(partitions.size());

        System.out.println("开始 MySQL 并行导入 users，分区数: " + partitions.size());

        for (int i = 0; i < partitions.size(); i++) {
            final int partitionIndex = i;
            List<String[]> batch = partitions.get(i);

            pool.submit(() -> {
                Connection conn = null;
                int batchUsers = 0;
                int batchSkipped = 0;
                long threadStartTime = System.currentTimeMillis();

                try {
                    // 使用连接池获取连接
                    conn = getConnectionFromPool();

                    try (PreparedStatement userStmt = conn.prepareStatement(getInsertSQLUsers())) {
                        int batchCount = 0;

                        for (String[] cols : batch) {
                            try {
                                List<String> c = Safety.padToExpected(cols, EXPECTED_COLUMNS);
                                Integer authorId = Safety.safeInt(c.get(0));

                                if (authorId == null) {
                                    batchSkipped++;
                                    continue;
                                }
                                fillPreparedStatementForUsers(userStmt, c);
                                userStmt.addBatch();
                                batchCount++;

                                // 批处理 - 保持原有逻辑
                                if (batchCount % 1000 == 0) {
                                    int[] results = userStmt.executeBatch();
                                    for (int result : results) {
                                        if (result > 0) {
                                            batchUsers++;
                                        }
                                    }
                                    conn.commit();
                                    userStmt.clearBatch();
                                    batchCount = 0;
                                }
                            } catch (Exception ex) {
                                batchSkipped++;
                                System.err.println("分区 " + partitionIndex + " 插入失败: " + ex.getMessage());
                                try {
                                    conn.rollback();
                                    userStmt.clearBatch();
                                    batchCount = 0;
                                } catch (SQLException rollbackEx) {
                                    System.err.println("回滚失败: " + rollbackEx.getMessage());
                                }
                            }
                        }

                        if (batchCount > 0) {
                            try {
                                int[] results = userStmt.executeBatch();
                                for (int result : results) if (result > 0) batchUsers++;
                                conn.commit();
                            } catch (SQLException ex) {
                                System.err.println("最后一批提交失败: " + ex.getMessage());
                                conn.rollback();
                            }
                        }

                    }
                } catch (Exception e) {
                    System.err.println("分区 " + partitionIndex + " 数据库连接失败: " + e.getMessage());
                    batchSkipped = batch.size();
                } finally {
                    // 归还连接到连接池
                    returnConnectionToPool(conn);

                    usersInserted.addAndGet(batchUsers);
                    skipped.addAndGet(batchSkipped);
                    long threadEndTime = System.currentTimeMillis();
                    long threadTime = threadEndTime - threadStartTime;

                    // 显示进度
                    System.out.printf("分区 %d 完成: 处理 %d 条, 耗时 %d ms, 速度: %.2f 条/秒%n",
                            partitionIndex, batchUsers, threadTime,
                            (batchUsers * 1000.0) / threadTime);
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();

        // 关闭连接池
        closeConnectionPool();

        long totalEndTime = System.currentTimeMillis();
        long totalTime = totalEndTime - totalStartTime;

        System.out.println("=========================================");
        System.out.println("MySQL 用户数据导入完成统计:");
        System.out.println("总耗时: " + totalTime + " ms");
        System.out.println("处理记录: " + usersInserted.get() + " 条");
        System.out.println("跳过记录: " + skipped.get() + " 条");
        System.out.printf("平均速度: %.2f 条/秒%n", (usersInserted.get() * 1000.0) / totalTime);
        System.out.println("=========================================");
    }

    public static void importUserRelated(List<String[]> rows) throws Exception {
        // 初始化连接池
        initializeConnectionPool();

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger followersInserted = new AtomicInteger(0);
        AtomicInteger followingInserted = new AtomicInteger(0);

        List<List<String[]>> partitions = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += BATCH_SIZE) {
            partitions.add(rows.subList(i, Math.min(i + BATCH_SIZE, rows.size())));
        }

        CountDownLatch latch = new CountDownLatch(partitions.size());
        System.out.println("开始 MySQL 并行导入 followers 和 following。分区数: " + partitions.size());

        for (int i = 0; i < partitions.size(); i++) {
            final int partitionIndex = i;
            List<String[]> batch = partitions.get(i);

            pool.submit(() -> {
                Connection conn = null;
                int batchFollowers = 0;
                int batchFollowing = 0;
                int batchSkipped = 0;

                try {
                    // 使用连接池获取连接
                    conn = getConnectionFromPool();

                    try (PreparedStatement followerStmt = conn.prepareStatement(
                            "INSERT INTO user_followers (user_id, follower_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE user_id=user_id");
                         PreparedStatement followingStmt = conn.prepareStatement(
                                 "INSERT INTO user_following (user_id, following_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE user_id=user_id")) {

                        conn.setAutoCommit(false);
                        int batchCountFollowers = 0;
                        int batchCountFollowing = 0;

                        for (String[] cols : batch) {
                            try {
                                List<String> c = Safety.padToExpected(cols, EXPECTED_COLUMNS);
                                Integer authorId = Safety.safeInt(c.get(0));

                                if (authorId == null) continue;

                                // 处理 followers
                                String followerUsers = c.get(6);
                                if (followerUsers != null && !followerUsers.trim().isEmpty()) {
                                    List<Integer> followerIds = Safety.parseIds(followerUsers);
                                    for (Integer followerId : followerIds) {
                                        if (followerId != null) {
                                            followerStmt.setInt(1, authorId);
                                            followerStmt.setInt(2, followerId);
                                            followerStmt.addBatch();
                                            batchCountFollowers++;

                                            if (batchCountFollowers % 1000 == 0) {
                                                int[] results = followerStmt.executeBatch();
                                                for (int result : results) {
                                                    if (result > 0) batchFollowers++;
                                                }
                                                conn.commit();
                                                followerStmt.clearBatch();
                                                batchCountFollowers = 0;
                                            }
                                        }
                                    }
                                }

                                // 处理 following
                                String followingUsers = c.get(7);
                                if (followingUsers != null && !followingUsers.trim().isEmpty()) {
                                    List<Integer> followingIds = Safety.parseIds(followingUsers);
                                    for (Integer followingId : followingIds) {
                                        if (followingId != null && !followingId.equals(authorId)) {
                                            followingStmt.setInt(1, authorId);
                                            followingStmt.setInt(2, followingId);
                                            followingStmt.addBatch();
                                            batchCountFollowing++;

                                            if (batchCountFollowing % 1000 == 0) {
                                                int[] results = followingStmt.executeBatch();
                                                for (int result : results) {
                                                    if (result > 0) batchFollowing++;
                                                }
                                                conn.commit();
                                                followingStmt.clearBatch();
                                                batchCountFollowing = 0;
                                            }
                                        }
                                    }
                                }
                            } catch (Exception ex) {
                                batchSkipped++;
                                System.err.println("分区 " + partitionIndex + " 插入失败: " + ex.getMessage());
                            }
                        }

                        // 处理剩余批次
                        if (batchCountFollowers > 0) {
                            try {
                                int[] results = followerStmt.executeBatch();
                                for (int result : results) if (result > 0) batchFollowers++;
                                conn.commit();
                            } catch (SQLException ex) {
                                System.err.println("最后一批 followers 提交失败: " + ex.getMessage());
                                conn.rollback();
                            }
                        }

                        if (batchCountFollowing > 0) {
                            try {
                                int[] results = followingStmt.executeBatch();
                                for (int result : results) if (result > 0) batchFollowing++;
                                conn.commit();
                            } catch (SQLException ex) {
                                System.err.println("最后一批 following 提交失败: " + ex.getMessage());
                                conn.rollback();
                            }
                        }

                    }
                } catch (Exception e) {
                    System.err.println("分区 " + partitionIndex + " 关系插入失败: " + e.getMessage());
                } finally {
                    returnConnectionToPool(conn);
                    followersInserted.addAndGet(batchFollowers);
                    followingInserted.addAndGet(batchFollowing);
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();
        closeConnectionPool();

        System.out.println("MySQL 关系数据导入完成: followers = " + followersInserted.get() +
                ", following = " + followingInserted.get());
    }

    private static String getInsertSQLUsers() {
        // 修改为 MySQL 的冲突处理语法
        return """
        INSERT INTO users (
            author_id, author_name, gender, age, followers_count, following_count, user_followers, user_following) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE author_id=author_id
        """;
    }

    public static void fillPreparedStatementForUsers(PreparedStatement ps, List<String> c) throws SQLException {
        ps.setObject(1, Safety.safeInt(c.get(0)));                    // author_id
        ps.setString(2, Safety.safeStr(c.get(1)));                 // author_name
        ps.setString(3, Safety.safeStr(c.get(2)));                 // gender
        ps.setObject(4, Safety.safeInt(c.get(3)));                 // age
        ps.setObject(5, Safety.safeInt(c.get(4)));              // followers_count
        ps.setObject(6, Safety.safeInt(c.get(5)));              // following_count
        ps.setString(7, Safety.safeStr(c.get(6)));              // follower_users
        ps.setString(8, Safety.safeStr(c.get(7)));              // following_users
    }

    public static void dropUserColumns() throws SQLException, InterruptedException {
        Connection conn = null;
        try {
            conn = getConnectionFromPool();
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("ALTER TABLE users DROP COLUMN user_followers");
                stmt.executeUpdate("ALTER TABLE users DROP COLUMN user_following");
                conn.commit();
            }
        } finally {
            returnConnectionToPool(conn);
        }
    }
}