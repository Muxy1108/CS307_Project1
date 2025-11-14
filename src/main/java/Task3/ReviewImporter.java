package Task3;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ReviewImporter {

    private static final int EXPECTED_COLUMNS = 9;
    private static final int BATCH_SIZE = 1000;
    private static final int THREAD_COUNT = 6;
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String JDBC_USER = "postgres";
    private static final String JDBC_PASS = "Xieyan2005";


    public static void createReviewTable() throws SQLException {

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS reviews (" +
                    "review_id INTEGER PRIMARY KEY NOT NULL, " +
                    "recipe_id INTEGER, " +
                    "author_id INTEGER NOT NULL, " +
                    "author_name text , " +
                    "rating    INTEGER CHECK (rating BETWEEN 0 AND 5)," +
                    "review_content text, " +
                    "date_submitted date," +
                    "date_modified date," +
                    "Likes text," +
                    //"FOREIGN KEY(author_id) references users(author_id)," +
                    //"FOREIGN KEY(recipe_id) references recipes(recipe_id)" +
                    ")";
            stmt.execute(sql);
            System.out.println("review 表创建完成");
        }
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS review_likers (" +
                    "like_id SERIAL PRIMARY KEY, " +
                    "review_id INTEGER, " +
                    "liker_id INTEGER NOT NULL," +
                    //"FOREIGN KEY (review_id) REFERENCES reviews(review_id)," +
                    //"FOREIGN KEY (liker_id) REFERENCES users(author_id)" +
                    ")";
            stmt.execute(sql);
            System.out.println("review_likers 表创建完成");
        }

    }

    public static void importReviewData(List<String[]> rows) throws Exception {

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger reviewsInserted = new AtomicInteger(0);
        AtomicInteger skipped = new AtomicInteger(0);
        long totalStartTime = System.currentTimeMillis();

        List<List<String[]>> partitions = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += BATCH_SIZE) {
            partitions.add(rows.subList(i, Math.min(i + BATCH_SIZE, rows.size())));
        }

        CountDownLatch latch = new CountDownLatch(partitions.size());

        System.out.println("开始并行导入reviews，分区数: " + partitions.size());

        for (int i = 0; i < partitions.size(); i++) {
            final int partitionIndex = i;
            List<String[]> batch = partitions.get(i);

            pool.submit(() -> {
                int batchReviews = 0;
                int batchSkipped = 0;
                int batchReviewProcessed = 0;
                long threadStartTime = System.currentTimeMillis();

                try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
                     PreparedStatement reviewStmt = conn.prepareStatement(getInsertSQLReviews())){
                    conn.setAutoCommit(false);
                    int batchCount = 0;
                    long batchStartTime = System.currentTimeMillis();
                    for (String[] cols : batch) {
                        try {
                            List<String> c = Safety.padToExpected(cols,EXPECTED_COLUMNS);
                            Integer reviewId = Safety.safeInt(c.get(0));

                            if (reviewId == null) {
                                batchSkipped++;
                                continue;
                            }
                            fillPreparedStatementForReviews(reviewStmt, c);
                            reviewStmt.addBatch();
                            batchCount++;
                            batchReviewProcessed++;
                            //int userResult = reviewStmt.executeUpdate();

                            if (batchCount % 1000 == 0) {
                                int[] results = reviewStmt.executeBatch();
                                for (int result : results) {
                                    if (result > 0) {
                                        batchReviews++;
                                    }
                                }
                                conn.commit();
                                reviewStmt.clearBatch();
                                batchCount = 0;
                                long currentTime = System.currentTimeMillis();
                                double speed = 1000.0 / ((currentTime - batchStartTime) / 1000.0);
                                System.out.printf("分区 %d: 已处理 %d 条评论, 速度: %.2f 条/秒%n",
                                        partitionIndex, batchReviews, speed);
                                batchStartTime = currentTime;
                            }

                        }catch(Exception ex){
                            batchSkipped ++;
                            System.err.println("分区 " + partitionIndex + " 插入失败: " + ex.getMessage());
                            try {
                                conn.rollback();
                                reviewStmt.clearBatch();
                                batchCount = 0;
                            } catch (SQLException rollbackEx) {
                                System.err.println("回滚失败: " + rollbackEx.getMessage());
                            }
                        }
                    }
                    if(batchCount > 0){
                        try{
                            int[] results = reviewStmt.executeBatch();
                            for (int result : results) if (result > 0) batchReviews++;
                            conn.commit();
                        }catch(SQLException ex){
                            System.err.println("最后一批提交失败: " + ex.getMessage());
                            conn.rollback();
                        }
                    }

                } catch (Exception e) {
                    System.err.println("分区 " + partitionIndex + " 数据库连接失败: " + e.getMessage());
                    batchSkipped = batch.size();
                } finally {
                    reviewsInserted.addAndGet(batchReviews);
                    skipped.addAndGet(batchSkipped);
                    latch.countDown();
                }
            });
        }
        latch.await();
        pool.shutdown();

        long totalEndTime = System.currentTimeMillis();
        long totalTime = totalEndTime - totalStartTime;

        System.out.println("=========================================");
        System.out.println("评论数据导入完成统计:");
        System.out.println("总耗时: " + totalTime + " ms");
        System.out.println("处理记录: " + reviewsInserted.get() + " 条");
        System.out.println("跳过记录: " + skipped.get() + " 条");
        System.out.printf("平均速度: %.2f 条/秒%n", (reviewsInserted.get() * 1000.0) / totalTime);
        System.out.println("=========================================");



        System.out.println("关系数据导入完成: Reviews = " + reviewsInserted.get());

    }
    public static void importReviewRelated(List<String[]> rows) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger likesInserted = new AtomicInteger(0);
        AtomicInteger Skipped = new AtomicInteger(0);

        List<List<String[]>> partitions = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += BATCH_SIZE) {
            partitions.add(rows.subList(i, Math.min(i + BATCH_SIZE, rows.size())));
        }

        CountDownLatch latch = new CountDownLatch(partitions.size());

        System.out.println("开始并行导入Likes ，分区数: " + partitions.size());

        for (int i = 0; i < partitions.size(); i++) {
            final int partitionIndex = i;
            List<String[]> batch = partitions.get(i);

            pool.submit(() -> {
                int batchLikes = 0;
                int batchSkipped = 0;


                try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
                     PreparedStatement likeStmt = conn.prepareStatement(
                             "INSERT INTO review_likers (review_id, liker_id) VALUES (?, ?) ON CONFLICT DO NOTHING")) {

                    for (String[] cols : batch) {
                        try {
                            List<String> c = Safety.padToExpected(cols,EXPECTED_COLUMNS);
                            Integer reviewId = Safety.safeInt(c.get(0));

                            if (reviewId == null) {
                                batchSkipped ++;
                                continue;
                            }

                            String likers = c.get(8);
                            if (likers != null && !likers.trim().isEmpty()) {

                                List<Integer> liker_Ids = Safety.parseIds(likers);

                                if (!liker_Ids.isEmpty()) {

                                    for (Integer liker_Id : liker_Ids) {

                                        likeStmt.setInt(1, reviewId);
                                        likeStmt.setInt(2, liker_Id);
                                        int likerResult = likeStmt.executeUpdate();
                                        if (likerResult > 0) {
                                            batchLikes++;
                                        }
                                    }
                                }
                            }
                        }catch (Exception ex){
                            batchSkipped ++;
                            System.err.println("分区 " + partitionIndex + " 插入失败: " + ex.getMessage());
                        }
                    }
                } catch (Exception e) {
                    System.err.println("分区 " + partitionIndex + " 关系插入失败: " + e.getMessage());
                } finally {
                    likesInserted.addAndGet(batchLikes);
                    Skipped.addAndGet(batchSkipped);
                    latch.countDown();
                }
            });
        }
        latch.await();
        pool.shutdown();
        System.out.println("关系数据导入完成: Likes = " + likesInserted.get());

    }


    public static String getInsertSQLReviews() {
        return """
        INSERT INTO reviews (
            review_id, recipe_id, author_id, author_name, rating, review_content, date_submitted, date_modified,Likes) 
            VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT (review_id) DO NOTHING
    """;
    }

    public static void fillPreparedStatementForReviews(PreparedStatement ps, List<String> c) throws SQLException {
        ps.setObject(1, Safety.safeInt(c.get(0)));                    // review_id
        ps.setObject(2, Safety.safeInt(c.get(1)));                 // recipe_id
        ps.setObject(3, Safety.safeInt(c.get(2)));                 // author_id
        ps.setString(4, Safety.safeStr(c.get(3)));              //author_name
        ps.setObject(5, Safety.safeInt(c.get(4)));                 //rating
        ps.setString(6, Safety.safeStr(c.get(5)));              // review_content
        ps.setObject(7, Safety.parseTimestamp(c.get(6)));              // date_submitted
        ps.setObject(8, Safety.parseTimestamp(c.get(7)));              // date_modified
        ps.setString(9, Safety.safeStr(c.get(8)));              // Likes

    }

    public static void dropReviewColumns() throws SQLException{
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("ALTER TABLE reviews DROP COLUMN author_name");
            stmt.executeUpdate("ALTER TABLE reviews DROP COLUMN Likes");
        }

    }
}
