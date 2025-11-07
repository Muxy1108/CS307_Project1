import java.io.*;
import java.sql.*;
import java.util.*;

public class Test {
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String JDBC_USER = "postgres";
    private static final String JDBC_PASS = "Xieyan2005";

    static class User{
        int  authorId, age, followersCount, followingCount;
        String authorName, gender, userFollowers, userFollowing;

        public User(int id, int authorId, String authorName, String gender, int age,
                    int followersCount, int followingCount, String userFollowers, String userFollowing) {

            this.authorId = authorId;
            this.authorName = authorName;
            this.gender = gender;
            this.age = age;
            this.followersCount = followersCount;
            this.followingCount = followingCount;
            this.userFollowers = userFollowers;
            this.userFollowing = userFollowing;
        }
    }

    public static void main(String[] args) throws Exception {

        Safety.deleteAll();

        List<User> users = readCSVData("data/user.csv");
        System.out.println("读取测试数据: " + users.size() + " 条记录");

        createTestTable();

        runDBPerformanceTest(users);
    }

    public static List<User> readCSVData(String filename) throws IOException {
        List<User> users = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line = br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",", -1);
                if (fields.length >= 9) {
                    users.add(new User(
                            parseIntSafe(fields[0]),
                            parseIntSafe(fields[1]),
                            fields[2],
                            fields[3],
                            parseIntSafe(fields[4]),
                            parseIntSafe(fields[5]),
                            parseIntSafe(fields[6]),
                            fields[7],
                            fields[8]
                    ));
                }
            }
        }
        return users.subList(0, Math.min(users.size(), 5000)); // 限制数据量
    }

    private static int parseIntSafe(String s) {
        try {
            return s != null && !s.trim().isEmpty() ? Integer.parseInt(s.trim()) : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    public static void createTestTable() throws SQLException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {

            stmt.execute("DROP TABLE IF EXISTS users_test");
            stmt.execute("""
                CREATE TABLE users_test (
                    author_id INTEGER PRIMARY KEY,
                    author_name VARCHAR(100),
                    gender VARCHAR(10),
                    age INTEGER,
                    followers_count INTEGER,
                    following_count INTEGER,
                    user_followers TEXT,
                    user_following TEXT
                )
            """);

            //stmt.execute("CREATE INDEX idx_age ON users_test(age)");
            //stmt.execute("CREATE INDEX idx_followers ON users_test(followers_count)");

            System.out.println("测试表创建完成");
        }
    }

    public static void runDBPerformanceTest(List<User> users) throws SQLException {
        System.out.println("\n开始DBMS性能测试...");

        // 测试1: 批量插入
        long insertTime = testBatchInsert(users);

        // 测试2: 查询操作
        long selectAllTime = testSelectAll();
        long selectAgeTime = testSelectByAge();
        long selectFollowersTime = testSelectByFollowers();

        // 测试3: 更新操作
        long updateTime = testUpdate();

        // 测试4: 删除操作
        long deleteTime = testDelete();

        // 测试5: 复杂查询
        long complexQueryTime = testComplexQuery();

        // 输出结果
        printResults(insertTime, selectAllTime, selectAgeTime, selectFollowersTime,
                updateTime, deleteTime, complexQueryTime, users.size());
    }

    private static long testBatchInsert(List<User> users) throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             PreparedStatement pstmt = conn.prepareStatement(
                     "INSERT INTO users_test VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {

            for (User user : users) {
                pstmt.setInt(1, user.authorId);
                pstmt.setString(2, user.authorName);
                pstmt.setString(3, user.gender);
                pstmt.setInt(4, user.age);
                pstmt.setInt(5, user.followersCount);
                pstmt.setInt(6, user.followingCount);
                pstmt.setString(7, user.userFollowers);
                pstmt.setString(8, user.userFollowing);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long testSelectAll() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM users_test")) {

            int count = 0;
            while (rs.next()) {
                count++; // 模拟数据处理
            }
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long testSelectByAge() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT * FROM users_test WHERE age BETWEEN 25 AND 35")) {

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                // 处理结果
            }
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long testSelectByFollowers() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT * FROM users_test WHERE followers_count > 1000")) {

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                // 处理结果
            }
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long testUpdate() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("UPDATE users_test SET followers_count = followers_count + 10 WHERE age < 30");
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long testDelete() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("DELETE FROM users_test WHERE gender = 'Male' AND age > 50");
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long testComplexQuery() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("""
                 SELECT gender, AVG(followers_count) as avg_followers, COUNT(*) as count
                 FROM users_test 
                 WHERE age BETWEEN 20 AND 40 
                 GROUP BY gender 
                 HAVING COUNT(*) > 10
             """)) {

            while (rs.next()) {
                // 处理结果
            }
        }

        return System.currentTimeMillis() - startTime;
    }

    public static void printResults(long insertTime, long selectAllTime, long selectAgeTime,
                                    long selectFollowersTime, long updateTime, long deleteTime,
                                    long complexQueryTime, int totalRecords) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("DBMS 性能测试结果");
        System.out.println("=".repeat(60));
        System.out.printf("%-20s %-15s %-15s%n", "操作类型", "执行时间(ms)", "处理记录数");
        System.out.println("-".repeat(60));

        System.out.printf("%-20s %-15d %-15d%n", "批量插入", insertTime, totalRecords);
        System.out.printf("%-20s %-15d %-15s%n", "全表查询", selectAllTime, "全部");
        System.out.printf("%-20s %-15d %-15s%n", "年龄条件查询", selectAgeTime, "25-35岁");
        System.out.printf("%-20s %-15d %-15s%n", "粉丝数查询", selectFollowersTime, ">1000");
        System.out.printf("%-20s %-15d %-15s%n", "更新操作", updateTime, "年龄<30");
        System.out.printf("%-20s %-15d %-15s%n", "删除操作", deleteTime, "男性>50岁");
        System.out.printf("%-20s %-15d %-15s%n", "复杂聚合查询", complexQueryTime, "分组统计");

        // 计算插入速度
        double insertSpeed = (totalRecords * 1000.0) / insertTime;
        System.out.printf("\n插入速度: %.2f 条/秒%n", insertSpeed);
    }
}