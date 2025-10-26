import com.opencsv.*;
import com.opencsv.CSVReader;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class RecipeImporter {

    private static final int EXPECTED_COLUMNS = 27;
    private static final int BATCH_SIZE = 1000;
    private static final int THREAD_COUNT = 6;
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String JDBC_USER = "postgres";
    private static final String JDBC_PASS = "Xieyan2005";


    public static void createRecipeTable() throws SQLException {

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS recipes (" +
                    "recipe_id INTEGER PRIMARY KEY NOT NULL, " +
                    "recipe_name text NOT NULL, " +
                    "author_id INTEGER, " +
                    "author_name text, " +
                    "cook_time text, " +
                    "prep_time text, " +
                    "total_time text, " +
                    "date_published DATE, " +
                    "description TEXT, " +
                    "recipe_category text, " +
                    "keywords TEXT, " +
                    "recipe_ingredient TEXT, " +
                    "aggregated_rating NUMERIC(3,1) CHECK (aggregated_rating BETWEEN 0 AND 5), " +
                    "review_count INTEGER DEFAULT 0, " +
                    "calories INTEGER CHECK (calories >= 0), " +
                    "fat_content INTEGER DEFAULT 0, " +
                    "saturated_fat_content INTEGER DEFAULT 0, " +
                    "cholesterol_content INTEGER DEFAULT 0, " +
                    "sodium_content INTEGER DEFAULT 0, " +
                    "carbohydrate_content INTEGER DEFAULT 0, " +
                    "fiber_content INTEGER DEFAULT 0, " +
                    "sugar_content INTEGER DEFAULT 0, " +
                    "protein_content INTEGER DEFAULT 0, " +
                    "recipe_servings INTEGER CHECK (recipe_servings > 0), " +
                    "recipe_yield text, " +
                    "recipe_instructions TEXT, " +
                    "favorite_users TEXT, " +
                    "FOREIGN KEY (author_id) REFERENCES users(author_id) " +
                    //"FOREIGN KEY (author_name) REFERENCES users(author_name)" +
                    ")";
            stmt.execute(sql);
            System.out.println("recipes 表创建完成");

            String keywords_sql = "CREATE TABLE IF NOT EXISTS keywords (" +
                    "keyword_id SERIAL primary key not null, " +
                    "recipe_id  INTEGER            not null, " +
                    "keyword_text TEXT             not null, " +
                    "FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id) ON DELETE CASCADE" +
                    ")";
            stmt.execute(keywords_sql);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_keywords_recipe_id ON keywords(recipe_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_keywords_keyword_text ON keywords(keyword_text)");
            System.out.println("keywords 表创建完成");


            String ingredient_sql = "CREATE TABLE IF NOT EXISTS recipe_ingredients (" +
                    "ingredient_id SERIAL PRIMARY KEY NOT NULL, " +
                    "recipe_id INTEGER NOT NULL, " +
                    "ingredient_text TEXT NOT NULL, " +
                    "FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id)" +
                    ")";
            stmt.execute(ingredient_sql);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_ingredients_recipe_id ON recipe_ingredients(recipe_id)");
            System.out.println("recipe_ingredients 表创建完成");


            String instruction_sql = "CREATE TABLE IF NOT EXISTS recipe_instructions (" +
                    "instruction_id SERIAL PRIMARY KEY NOT NULL, " +
                    "recipe_id INTEGER NOT NULL, " +
                    "step_order INTEGER NOT NULL, " +
                    "instruction_text TEXT NOT NULL, " +
                    "FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id)" +
                    ")";
            stmt.execute(instruction_sql);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_instructions_recipe_id ON recipe_instructions(recipe_id)");
            System.out.println("recipe_instructions 表创建完成");

        }
    }


    public static void importRecipeData(List<String[]> rows) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger recipesInserted = new AtomicInteger(0);
        AtomicInteger skipped = new AtomicInteger(0);

        List<List<String[]>> partitions = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += BATCH_SIZE) {
            partitions.add(rows.subList(i, Math.min(i + BATCH_SIZE, rows.size())));
        }

        CountDownLatch latch = new CountDownLatch(partitions.size());

        System.out.println("开始并行导入recipes，分区数: " + partitions.size());

        for (int i = 0; i < partitions.size(); i++) {
            final int partitionIndex = i;
            List<String[]> batch = partitions.get(i);

            pool.submit(() -> {
                int batchRecipes = 0;
                int batchSkipped = 0;

                try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
                     PreparedStatement recipeStmt = conn.prepareStatement(getInsertSQLRecipes())) {

                    for (String[] cols : batch) {
                        try {
                            List<String> c = Safety.padToExpected(cols,EXPECTED_COLUMNS);
                            Integer recipeId = Safety.safeInt(c.get(0));

                            if (recipeId == null) {
                                batchSkipped++;
                                continue;
                            }

                            fillPreparedStatementRecipe(recipeStmt, c);
                            int recipeResult = recipeStmt.executeUpdate();

                            if (recipeResult > 0) {
                                batchRecipes++;
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
                    recipesInserted.addAndGet(batchRecipes);
                    skipped.addAndGet(batchSkipped);
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();

        System.out.println("Recipes导入完成. " +
                "recipes = " + recipesInserted.get() + ", " +
                "skipped = " + skipped.get());

    }

    public static void importRecipeRelated(List<String[]> rows) throws Exception {

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);

        AtomicInteger keywordsInserted = new AtomicInteger(0);
        AtomicInteger ingredientsInserted = new AtomicInteger(0);
        AtomicInteger instructionsInserted = new AtomicInteger(0);
        AtomicInteger skipped = new AtomicInteger(0);
        AtomicInteger recipesProcessed = new AtomicInteger(0);

        List<List<String[]>> partitions = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += BATCH_SIZE) {
            partitions.add(rows.subList(i, Math.min(i + BATCH_SIZE, rows.size())));
        }

        CountDownLatch latch = new CountDownLatch(partitions.size());

        System.out.println("开始并行导入所有关联数据，分区数: " + partitions.size());

        for (int i = 0; i < partitions.size(); i++) {
            final int partitionIndex = i;
            List<String[]> batch = partitions.get(i);

            pool.submit(() -> {
                int batchKeywords = 0;
                int batchIngredients = 0;
                int batchInstructions = 0;
                int batchSkipped = 0;
                int batchRecipesProcessed = 0;

                try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
                     PreparedStatement keywordStmt = conn.prepareStatement(
                             "INSERT INTO keywords (recipe_id, keyword_text) VALUES (?, ?) ON CONFLICT DO NOTHING");
                     PreparedStatement ingredientStmt = conn.prepareStatement(
                             "INSERT INTO recipe_ingredients (recipe_id, ingredient_text) VALUES (?, ?) ON CONFLICT DO NOTHING");
                     PreparedStatement instructionStmt = conn.prepareStatement(
                             "INSERT INTO recipe_instructions (recipe_id, step_order, instruction_text) VALUES (?, ?, ?) ON CONFLICT DO NOTHING")) {

                    for (String[] cols : batch) {
                        try {
                            List<String> c = Safety.padToExpected(cols,EXPECTED_COLUMNS);
                            Integer recipeId = Safety.safeInt(c.get(0));

                            if (recipeId == null) {
                                batchSkipped++;
                                continue;
                            }

                            batchRecipesProcessed++;

                            String rawKeywords = c.get(10);
                            if (rawKeywords != null && !rawKeywords.trim().isEmpty()) {
                                List<String> keywords = Safety.parseStrs(rawKeywords);
                                if (!keywords.isEmpty()) {
                                    for (String keyword : keywords) {
                                        if (keyword.length() > 255) {
                                            keyword = keyword.substring(0, 255);
                                        }
                                        keywordStmt.setInt(1, recipeId);
                                        keywordStmt.setString(2, keyword);
                                        int keywordResult = keywordStmt.executeUpdate();
                                        if (keywordResult > 0) {
                                            batchKeywords++;
                                        }
                                    }
                                }
                            }


                            String rawIngredients = Safety.normalizeArray(c.get(11));
                            if (rawIngredients != null && !rawIngredients.trim().isEmpty()){
                                List<String> ingredients = Safety.parseStrs(rawIngredients);
                                if (!ingredients.isEmpty()){
                                    for (String ingredient : ingredients){
                                        if (ingredient != null && !ingredient.trim().isEmpty()){
                                            ingredientStmt.setInt(1, recipeId);
                                            ingredientStmt.setString(2, ingredient.trim());
                                            int ingredientResult = ingredientStmt.executeUpdate();
                                            if (ingredientResult > 0) {
                                                batchIngredients++;
                                            }
                                        }
                                    }
                                }
                            }


                            String rawInstructions = Safety.safeStr(c.get(25));
                            if (rawInstructions != null && !rawInstructions.trim().isEmpty()) {
                                List<String> instructions = Safety.parseStrs(rawInstructions);
                                if (!instructions.isEmpty()) {
                                    int stepOrder = 1;
                                    for (String instruction : instructions) {
                                        if (instruction != null && !instruction.trim().isEmpty()) {
                                            instructionStmt.setInt(1, recipeId);
                                            instructionStmt.setInt(2, stepOrder);
                                            instructionStmt.setString(3, instruction.trim());
                                            int instructionResult = instructionStmt.executeUpdate();
                                            if (instructionResult > 0) {
                                                batchInstructions++;
                                            }
                                            stepOrder++;
                                        }
                                    }
                                }
                            }
                        } catch (Exception ex) {
                            batchSkipped++;
                            System.err.println("分区 " + partitionIndex + " 处理失败: " + ex.getMessage());
                        }
                    }
                } catch (Exception e) {
                    System.err.println("分区 " + partitionIndex + " 数据库连接失败: " + e.getMessage());
                    batchSkipped = batch.size();
                } finally {
                    keywordsInserted.addAndGet(batchKeywords);
                    ingredientsInserted.addAndGet(batchIngredients);
                    instructionsInserted.addAndGet(batchInstructions);
                    skipped.addAndGet(batchSkipped);
                    recipesProcessed.addAndGet(batchRecipesProcessed);
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();

        System.out.println("=== recipe导入完成 ===");
        System.out.println("keywords导入: " + keywordsInserted.get() + " 条");
        System.out.println("ingredients导入: " + ingredientsInserted.get() + " 条");
        System.out.println("instructions导入: " + instructionsInserted.get() + " 条");
        System.out.println("recipe处理数 : " + recipesProcessed.get());
        System.out.println("跳过的记录: " + skipped.get() + " 条");
    }


    public static String getInsertSQLRecipes() {
        return """
        INSERT INTO recipes (
            recipe_id, recipe_name, author_id, author_name, cook_time, prep_time,
            total_time, date_published, description, recipe_category, keywords,
            recipe_ingredient, aggregated_rating, review_count, calories, fat_content,
            saturated_fat_content, cholesterol_content, sodium_content, carbohydrate_content,
            fiber_content, sugar_content, protein_content, recipe_servings, recipe_yield,
            recipe_instructions, favorite_users
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT (recipe_id) DO NOTHING
    """;
    }

    public static void fillPreparedStatementRecipe(PreparedStatement ps, List<String> c) throws SQLException {
        ps.setObject(1, Safety.safeInt(c.get(0))); //recipe_id
        ps.setString(2, Safety.safeStr(c.get(1))); //recipe_name
        ps.setObject(3, Safety.safeInt(c.get(2))); //author_id
        ps.setString(4, Safety.safeStr(c.get(3))); //author_name 删
        ps.setString(5, Safety.safeStr(c.get(4)));
        ps.setString(6, Safety.safeStr(c.get(5)));
        ps.setString(7, Safety.safeStr(c.get(6)));
        ps.setObject(8, Safety.parseTimestamp(c.get(7)));
        ps.setString(9, Safety.safeStr(c.get(8)));
        ps.setString(10, Safety.safeStr(c.get(9)));
        ps.setString(11, Safety.safeStr(c.get(10)));        // keywords!!!!
        ps.setString(12, Safety.normalizeArray(c.get(11))); // recipe_ingredient
        ps.setObject(13, Safety.safeDouble(c.get(12)));     // aggregated_rating
        ps.setObject(14, Safety.safeDouble(c.get(13)));     // review_count
        ps.setObject(15, Safety.safeDouble(c.get(14)));     // calories
        ps.setObject(16, Safety.safeDouble(c.get(15)));     // fat_content
        ps.setObject(17, Safety.safeDouble(c.get(16)));     // saturated_fat_content
        ps.setObject(18, Safety.safeDouble(c.get(17)));     // cholesterol_content
        ps.setObject(19, Safety.safeDouble(c.get(18)));     // sodium_content
        ps.setObject(20, Safety.safeDouble(c.get(19)));     // carbohydrate_content
        ps.setObject(21, Safety.safeDouble(c.get(20)));     // fiber_content
        ps.setObject(22, Safety.safeDouble(c.get(21)));     // sugar_content
        ps.setObject(23, Safety.safeDouble(c.get(22)));     // protein_content
        ps.setObject(24, Safety.safeDouble(c.get(23)));     // recipe_servings
        ps.setString(25, Safety.safeStr(c.get(24)));        // recipe_yield
        ps.setString(26, Safety.safeStr(c.get(25)));        // recipe_instructions
        ps.setString(27, Safety.normalizeArray(c.get(26))); // favorite_users
    }


    public static void verifyKeywordsImport() throws SQLException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {

            System.out.println("验证导入结果:");

            // 检查 recipes 数量
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM recipes");
            if (rs.next()) {
                System.out.println("recipes 表记录数: " + rs.getInt(1));
            }
        }
    }

    public static void dropRecipeLines() throws SQLException {

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("ALTER TABLE recipes DROP COLUMN IF EXISTS author_name");
            stmt.executeUpdate("ALTER TABLE recipes DROP COLUMN IF EXISTS keywords");
            stmt.executeUpdate("ALTER TABLE recipes DROP COLUMN IF EXISTS recipe_ingredient");
            stmt.executeUpdate("ALTER TABLE recipes DROP COLUMN IF EXISTS recipe_instructions");

        }
    }

}