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

    public static List<String[]> ReadUserCSV(String csvPath) {
        List<String[]> rows = new ArrayList<>();

        System.out.println("开始读取 CSV 文件...");

        CSVParser parser = new CSVParserBuilder()
                .withSeparator(',')
                .withQuoteChar('"')
                .withEscapeChar('\\')
                .withIgnoreQuotations(false)
                .withStrictQuotes(false)
                .build();

        try (BufferedReader br = new BufferedReader(new FileReader(csvPath));
             CSVReader reader = new CSVReaderBuilder(br)
                     .withCSVParser(parser)
                     .withSkipLines(0)
                     .build()) {

            String[] header = reader.readNext();
            if (header == null) {
                System.err.println("Empty CSV file.");
                return rows;
            }

            String[] line;
            int lineCount = 0;
            int errorCount = 0;

            while (true) {
                try {
                    line = reader.readNext();
                    if(line == null) break;

                    lineCount++;

                    if (line.length >= EXPECTED_COLUMNS) {
                        rows.add(line);
                    }else{
                        errorCount++;
                    }

                    if (lineCount % 10000 == 0) {
                        System.out.println("已读取行数: " + lineCount + ", 错误: " + errorCount);
                    }

                } catch (Exception e) {
                    errorCount ++;
                }
            }
            System.out.println("读取完成: 总行数 = " + lineCount + ", 有效行 = " + rows.size() + ", 错误 = " + errorCount);
        } catch (Exception e) {
            System.err.println("读取 CSV 文件失败: " + e.getMessage());
            e.printStackTrace();
        }
        return rows;
    }
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
                    "follower_users TEXT, " +
                    "following_users TEXT" +
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
                    "FOREIGN KEY (follower_id) REFERENCES users(author_id) ON DELETE CASCADE, " +
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
                    "FOREIGN KEY (following_id) REFERENCES users(author_id) ON DELETE CASCADE, " +
                    "UNIQUE(user_id, following_id)" +
                    ")";
            stmt.execute(sql);
            System.out.println("users_following 表创建完成");
        }

    }


    public static void importUserData(List<String[]> rows) throws Exception {

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
                int batchFollowers = 0;
                int batchFollowing = 0;

                try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);

                     PreparedStatement userStmt = conn.prepareStatement(getInsertSQLUsers());
                     PreparedStatement followerStmt = conn.prepareStatement(
                             "INSERT INTO user_followers (user_id, follower_id) VALUES (?, ?) ON CONFLICT DO NOTHING");
                     PreparedStatement followingStmt = conn.prepareStatement(
                             "INSERT INTO user_following (user_id, following_id) VALUES (?, ?) ON CONFLICT DO NOTHING")
                     ){

                    for (String[] cols : batch) {
                        try {
                            List<String> c = Safety.padToExpected(cols);
                            Integer authorId = Safety.safeInt(c.get(0));

                            if (authorId == null) {
                                batchSkipped ++;
                                continue;
                            }
                            fillPreparedStatementForUsers(userStmt, c);
                            int userResult = userStmt.executeUpdate();

                            if (userResult > 0) {
                                batchUsers ++;

                                String followerUsers = c.get(6);
                                if (followerUsers != null && !followerUsers.trim().isEmpty()) {
                                    batchUserProcessed ++;

                                    List<Integer> followerIds = parseUserIds(followerUsers);

                                    if (!followerIds.isEmpty()) {

                                        for (Integer followerId : followerIds) {


                                            followerStmt.setInt(1, authorId);
                                            followerStmt.setInt(2, followerId);
                                            int followersResult = followerStmt.executeUpdate();

                                            if (followersResult > 0) {
                                                batchFollowers ++;
                                            }
                                        }
                                    }
                                }
                                String followingUsers = c.get(7); // following_users 列
                                if (followingUsers != null && !followingUsers.trim().isEmpty()) {
                                    List<Integer> followingIds = parseUserIds(followingUsers);
                                    if (!followingIds.isEmpty()) {
                                        for (Integer followingId : followingIds) {
                                            if (followingId != null && !followingId.equals(authorId)) {
                                                followingStmt.setInt(1, authorId);
                                                followingStmt.setInt(2, followingId);
                                                int followingResult = followingStmt.executeUpdate();
                                                if (followingResult > 0) {
                                                    batchFollowing ++;
                                                }
                                            }
                                        }
                                    }
                                }
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
                    followersInserted.addAndGet(batchFollowers);
                    followingInserted.addAndGet(batchFollowing);
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

        verifyUsersImport();
    }

    public static List<Integer> parseUserIds(String userData) {
        List<Integer> userIds = new ArrayList<>();
        if (userData == null || userData.trim().isEmpty()) {
            return userIds;
        }

        try {
            String cleaned = userData.trim();
            if(cleaned.isEmpty() || cleaned.equals("null") || cleaned.equals("NULL")){
                return userIds;
            }

            String[] parts = cleaned.split(",");
            for (String part : parts) {
                String trimmed = part.trim();
                Integer userId = Safety.safeInt(trimmed);
                if (userId != null) {
                    userIds.add(userId);
                    System.out.println("【parseUserIds】成功解析: '" + trimmed + "' -> " + userId);
                }
            }
        } catch (Exception e) {
            System.err.println("解析用户ID失败: " + userData);
        }
        return userIds;
    }

    public static String getInsertSQLUsers() {
        return """
        INSERT INTO users (
            author_id, author_name, gender, age, followers_count, following_count, follower_users, following_users) 
            VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT (author_id) DO NOTHING
    """;
    }

    public static void fillPreparedStatementForUsers(PreparedStatement ps, List<String> c) throws SQLException {
        ps.setInt(1, Safety.safeInt(c.get(0)));                    // author_id
        ps.setString(2, Safety.safeStr(c.get(1)));                 // author_name
        ps.setString(3, Safety.safeStr(c.get(2)));                 // gender
        ps.setObject(4, Safety.safeInt(c.get(3)));                 // age
        ps.setObject(5, Safety.safeInt(c.get(4)));              // followers_count
        ps.setObject(6, Safety.safeInt(c.get(5)));              // following_count
        ps.setString(7, Safety.safeStr(c.get(6)));              // follower_users
        ps.setString(8, Safety.safeStr(c.get(7)));              // following_users

    }


    public static void verifyUsersImport() throws SQLException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {

            System.out.println("验证导入结果:");

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM users");
            if (rs.next()) {
                System.out.println("users 表记录数: " + rs.getInt(1));
            }
        }
    }

}
