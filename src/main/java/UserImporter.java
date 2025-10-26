import com.opencsv.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class UserImporter {
    private static final int EXPECTED_COLUMNS = 8;
    private static final int BATCH_SIZE = 1000;
    private static final int THREAD_COUNT = 6;
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String JDBC_USER = "postgres";
    private static final String JDBC_PASS = "Xieyan2005";


    public static void createUserTable() throws SQLException {

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
            Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS users (" +
                    "author_id INTEGER PRIMARY KEY NOT NULL, " +
                    "author_name VARCHAR(100) NOT NULL, " +
                    "gender VARCHAR(10) CHECK (gender IN ('Male', 'Female')), " +
                    "age INTEGER CHECK (age > 0 AND age <= 120), " +
                    "followers_count INTEGER DEFAULT 0, " +
                    "following_count INTEGER DEFAULT 0, " +
                    "user_followers TEXT, " +
                    "user_following TEXT" +
                    ")";
            stmt.execute(sql);
            System.out.println("users 表创建完成");
        }
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS user_followers (" +
                    "id SERIAL PRIMARY KEY, " +
                    "user_id INTEGER NOT NULL, " +
                    "follower_id INTEGER NOT NULL, " +
                    "FOREIGN KEY (user_id) REFERENCES users(author_id)," +
                    "FOREIGN KEY (follower_id) REFERENCES users(author_id), " +
                    "UNIQUE(user_id, follower_id)" +
                    ")";
            stmt.execute(sql);
            System.out.println("users_followers 表创建完成");
        }
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS user_following (" +
                    "id SERIAL PRIMARY KEY, " +
                    "user_id INTEGER NOT NULL, " +
                    "following_id INTEGER NOT NULL, " +
                    "FOREIGN KEY (user_id) REFERENCES users(author_id)," +
                    "FOREIGN KEY (following_id) REFERENCES users(author_id), " +
                    "UNIQUE(user_id, following_id)" +
                    ")";
            stmt.execute(sql);
            System.out.println("users_following 表创建完成");
        }

    }


    public static void importUserData(List<String[]> rows) throws Exception{

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger usersInserted = new AtomicInteger(0);
        AtomicInteger followersInserted = new AtomicInteger(0);
        AtomicInteger followingInserted = new AtomicInteger(0);
        AtomicInteger skipped = new AtomicInteger(0);

        List<List<String[]>> partitions = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += BATCH_SIZE) {
            partitions.add(rows.subList(i, Math.min(i + BATCH_SIZE, rows.size())));
        }

        CountDownLatch latch = new CountDownLatch(partitions.size());

        System.out.println("开始并行导入users，分区数: " + partitions.size());

        for (int i = 0; i < partitions.size(); i ++) {
            final int partitionIndex = i;
            List<String[]> batch = partitions.get(i);

            pool.submit(() -> {
                int batchUsers = 0;
                int batchSkipped = 0;
                int batchUserProcessed = 0;

                try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);

                     PreparedStatement userStmt = conn.prepareStatement(getInsertSQLUsers());

                     ){

                    for (String[] cols : batch) {
                        try {
                            List<String> c = Safety.padToExpected(cols,EXPECTED_COLUMNS);
                            Integer authorId = Safety.safeInt(c.get(0));

                            if (authorId == null) {
                                batchSkipped ++;
                                continue;
                            }
                            fillPreparedStatementForUsers(userStmt, c);
                            int userResult = userStmt.executeUpdate();

                            if (userResult > 0) {
                                batchUsers ++;
                            }
                        } catch (Exception ex) {
                            batchSkipped++;
                            System.err.println("分区 " + partitionIndex + " 插入失败: " + ex.getMessage());
                        }
                    }

                } catch (Exception e) {
                    System.err.println("分区 " + partitionIndex + " 数据库连接失败: " + e.getMessage());
                    batchSkipped = batch.size();
                } finally {
                    usersInserted.addAndGet(batchUsers);
                    skipped.addAndGet(batchSkipped);
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();

        System.out.println("导入完成. " +
                "users = " + usersInserted.get() + ", " +
                "followers = " + followersInserted.get() + ", " +
                "following = " + followingInserted.get() + ", " +
                "skipped = " + skipped.get());

    }

    public static void importUserRelated(List<String[]> rows) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger followersInserted = new AtomicInteger(0);
        AtomicInteger followingInserted = new AtomicInteger(0);

        List<List<String[]>> partitions = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += BATCH_SIZE) {
            partitions.add(rows.subList(i, Math.min(i + BATCH_SIZE, rows.size())));
        }

        CountDownLatch latch = new CountDownLatch(partitions.size());

        for (int i = 0; i < partitions.size(); i++) {
            final int partitionIndex = i;
            List<String[]> batch = partitions.get(i);

            pool.submit(() -> {
                int batchFollowers = 0;
                int batchFollowing = 0;

                try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
                     PreparedStatement followerStmt = conn.prepareStatement(
                             "INSERT INTO user_followers (user_id, follower_id) VALUES (?, ?) ON CONFLICT DO NOTHING");
                     PreparedStatement followingStmt = conn.prepareStatement(
                             "INSERT INTO user_following (user_id, following_id) VALUES (?, ?) ON CONFLICT DO NOTHING")) {

                    for (String[] cols : batch) {
                        try {
                            List<String> c = Safety.padToExpected(cols,EXPECTED_COLUMNS);
                            Integer authorId = Safety.safeInt(c.get(0));

                            if (authorId == null) continue;

                            String followerUsers = c.get(6);
                            if (followerUsers != null && !followerUsers.trim().isEmpty()) {
                                List<Integer> followerIds = Safety.parseIds(followerUsers);
                                for (Integer followerId : followerIds) {
                                    if (followerId != null) {
                                        followerStmt.setInt(1, authorId);
                                        followerStmt.setInt(2, followerId);
                                        try {
                                            int result = followerStmt.executeUpdate();
                                            if (result > 0) batchFollowers++;
                                        } catch (SQLException e) {
                                            if (!e.getMessage().contains("外键约束")) {
                                                System.err.println("followers 关系插入错误: " + e.getMessage());
                                            }
                                        }
                                    }
                                }
                            }

                            String followingUsers = c.get(7);
                            if (followingUsers != null && !followingUsers.trim().isEmpty()) {
                                List<Integer> followingIds = Safety.parseIds(followingUsers);
                                for (Integer followingId : followingIds) {
                                    if (followingId != null && !followingId.equals(authorId)) {
                                        followingStmt.setInt(1, authorId);
                                        followingStmt.setInt(2, followingId);
                                        try {
                                            int result = followingStmt.executeUpdate();
                                            if (result > 0) batchFollowing++;
                                        } catch (SQLException e) {
                                            if (!e.getMessage().contains("外键约束")) {
                                                System.err.println("关注关系插入错误: " + e.getMessage());
                                            }
                                        }
                                    }
                                }
                            }
                        }catch (Exception ex) {
                        }
                    }
                } catch (Exception e) {
                    System.err.println("分区 " + partitionIndex + " 关系插入失败: " + e.getMessage());
                } finally {
                    followersInserted.addAndGet(batchFollowers);
                    followingInserted.addAndGet(batchFollowing);
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();
        System.out.println("关系数据导入完成: followers = " + followersInserted.get() +
                ", following = " + followingInserted.get());
    }


    private static String getInsertSQLUsers() {
        return """
        INSERT INTO users (
            author_id, author_name, gender, age, followers_count, following_count, user_followers, user_following) 
            VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT (author_id) DO NOTHING
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

    public static void dropUserColumns() throws SQLException{
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("ALTER TABLE users DROP COLUMN user_followers");
            stmt.executeUpdate("ALTER TABLE users DROP COLUMN user_following");

        }

    }

}
