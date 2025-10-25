import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        //import recipes and keywords
        String csvPath = "data/recipes.csv";
        List<String[]> rows = RecipeKeywordsImporter.readCSVWithErrorHandling(csvPath);
        try {
            RecipeKeywordsImporter.debugKeywordsData(rows);
            RecipeKeywordsImporter.createKeywordsTable();
            RecipeKeywordsImporter.importDataWithKeywordsDebug(rows);
        }catch (Exception e) {
            System.err.println("导入失败: " + e.getMessage());
            e.printStackTrace();
        }
    }


}
