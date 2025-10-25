import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        //import recipes and keywords
//        String RecipecsvPath = "data/recipes.csv";
//        List<String[]> recipesrows = RecipeKeywordsImporter.ReadRecipeCSV(RecipecsvPath);
//        try {
//            //RecipeKeywordsImporter.debugKeywordsData(ecipesrows);
//            RecipeKeywordsImporter.createRecipeKeywordsTable();
//            RecipeKeywordsImporter.importRecipeKeywordData(recipesrows);
//        }catch (Exception e) {
//            System.err.println("导入失败: " + e.getMessage());
//            e.printStackTrace();
//        }


        //import recipes and keywords

        String UsercsvPath = "data/user.csv";
        List<String[]> userrows = UserImporter.ReadUserCSV(UsercsvPath);
        try {
            UserImporter.createUserTable();
            UserImporter.importUserData(userrows);
        }catch (Exception e) {
            System.err.println("导入失败: " + e.getMessage());
            e.printStackTrace();
        }

        //import recipes and keywords

    }


}
