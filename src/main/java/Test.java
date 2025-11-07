import java.io.*;
import java.sql.*;
import java.util.*;

public class Test {
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String JDBC_USER = "postgres";
    private static final String JDBC_PASS = "Xieyan2005";

    public static void main(String[] args) throws Exception {
        // 直接对现有的users表进行性能测试
        runDBPerformanceTest();
    }

    public static void runDBPerformanceTest() throws SQLException {
        System.out.println("开始DBMS性能测试（使用现有users表）...");

        // 测试1: 查询操作
        long selectAllTime = testSelectAll();
        long selectAgeTime = testSelectByAge();
        long selectFollowersTime = testSelectByFollowers();
        long selectGenderTime = testSelectByGender();

        // 测试2: 更新操作
        long updateTime = testUpdate();

        // 测试3: 删除操作（使用临时数据）
        long deleteTime = testDelete();

        // 测试4: 复杂查询
        long complexQueryTime = testComplexQuery();
        long joinQueryTime = testJoinQuery();

        // 输出结果
        printResults(selectAllTime, selectAgeTime, selectFollowersTime, selectGenderTime,
                updateTime, deleteTime, complexQueryTime, joinQueryTime);
    }

    private static long testSelectAll() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM users")) {

            if (rs.next()) {
                System.out.println("全表记录数: " + rs.getInt(1));
            }
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long testSelectByAge() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT COUNT(*) FROM users WHERE age BETWEEN 25 AND 35")) {

            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                System.out.println("年龄25-35岁记录数: " + rs.getInt(1));
            }
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long testSelectByFollowers() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT COUNT(*) FROM users WHERE followers_count > 100")) {

            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                System.out.println("粉丝数>100记录数: " + rs.getInt(1));
            }
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long testSelectByGender() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT COUNT(*) FROM users WHERE gender = 'Female'")) {

            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                System.out.println("女性用户记录数: " + rs.getInt(1));
            }
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long testUpdate() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {

            int affected = stmt.executeUpdate(
                    "UPDATE users SET followers_count = followers_count + 1 WHERE age < 30");
            System.out.println("更新记录数: " + affected);
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long testDelete() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {

            // 创建一个临时副本进行删除测试，避免影响原数据
            int affected = stmt.executeUpdate(
                    "DELETE FROM users WHERE author_id IN (" +
                            "SELECT author_id FROM users WHERE age > 50 LIMIT 10)");
            System.out.println("删除记录数: " + affected);
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long testComplexQuery() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("""
                 SELECT gender, 
                        AVG(followers_count) as avg_followers, 
                        AVG(following_count) as avg_following,
                        COUNT(*) as count
                 FROM users 
                 WHERE age BETWEEN 20 AND 40 
                 GROUP BY gender 
                 HAVING COUNT(*) > 10
                 ORDER BY avg_followers DESC
             """)) {

            System.out.println("复杂查询结果:");
            while (rs.next()) {
                System.out.printf("  性别: %s, 平均粉丝: %.2f, 平均关注: %.2f, 数量: %d%n",
                        rs.getString("gender"),
                        rs.getDouble("avg_followers"),
                        rs.getDouble("avg_following"),
                        rs.getInt("count"));
            }
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long testJoinQuery() throws SQLException {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("""
                 SELECT u.gender, 
                        COUNT(DISTINCT uf.follower_id) as unique_followers,
                        AVG(u.followers_count) as avg_followers
                 FROM users u
                 LEFT JOIN user_followers uf ON u.author_id = uf.user_id
                 WHERE u.age BETWEEN 18 AND 60
                 GROUP BY u.gender
                 HAVING COUNT(DISTINCT uf.follower_id) > 0
                 ORDER BY unique_followers DESC
             """)) {

            System.out.println("连接查询结果:");
            while (rs.next()) {
                System.out.printf("  性别: %s, 独立粉丝数: %d, 平均粉丝: %.2f%n",
                        rs.getString("gender"),
                        rs.getInt("unique_followers"),
                        rs.getDouble("avg_followers"));
            }
        }

        return System.currentTimeMillis() - startTime;
    }

    public static void printResults(long selectAllTime, long selectAgeTime, long selectFollowersTime,
                                    long selectGenderTime, long updateTime, long deleteTime,
                                    long complexQueryTime, long joinQueryTime) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("DBMS 性能测试结果（现有users表）");
        System.out.println("=".repeat(60));
        System.out.printf("%-25s %-15s%n", "操作类型", "执行时间(ms)");
        System.out.println("-".repeat(60));

        System.out.printf("%-25s %-15d%n", "全表查询", selectAllTime);
        System.out.printf("%-25s %-15d%n", "年龄条件查询(25-35)", selectAgeTime);
        System.out.printf("%-25s %-15d%n", "粉丝数条件查询(>100)", selectFollowersTime);
        System.out.printf("%-25s %-15d%n", "性别条件查询(Female)", selectGenderTime);
        System.out.printf("%-25s %-15d%n", "更新操作", updateTime);
        System.out.printf("%-25s %-15d%n", "删除操作", deleteTime);
        System.out.printf("%-25s %-15d%n", "复杂聚合查询", complexQueryTime);
        System.out.printf("%-25s %-15d%n", "连接查询", joinQueryTime);

        // 计算查询性能对比
        System.out.println("\n性能分析:");
        System.out.printf("简单查询 vs 复杂查询: %.2fx%n", (double)complexQueryTime / selectAllTime);
        System.out.printf("条件查询 vs 全表查询: %.2fx%n", (double)selectAgeTime / selectAllTime);
    }
}