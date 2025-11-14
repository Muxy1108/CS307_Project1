package Task3;

import java.util.List;

public class Main {
    public static void main(String[] args){

        //delete all tables

        try{
            Safety.deleteAll();
        }catch (Exception e){
            System.err.println("数据库未清空！" + e.getMessage());
            e.printStackTrace();
        }

        //import users and user_follower/following

        String UsercsvPath = "data/user.csv";
        List<String[]> userrows = CSVReader.ReadCSV(UsercsvPath,8);
        try {
            UserImporter.createUserTable();
            UserImporter.importUserData(userrows);
            UserImporter.importUserRelated(userrows);
            UserImporter.dropUserColumns();
        }catch (Exception e) {
            System.err.println("users 导入失败: " + e.getMessage());
            e.printStackTrace();
        }

        //import recipes and keywords

        String RecipecsvPath = "data/recipes.csv";
        List<String[]> recipesrows = CSVReader.ReadCSV(RecipecsvPath,27);
        try {
            RecipeImporter.createRecipeTable();
            RecipeImporter.importRecipeData(recipesrows);
            RecipeImporter.importRecipeRelated(recipesrows);
            RecipeImporter.dropRecipeLines();
        }catch (Exception e) {
            System.err.println("recipes 导入失败: " + e.getMessage());
            e.printStackTrace();
        }

        //import reviews

        String ReviewscsvPath = "data/reviews.csv";
        List<String[]> reviewrows = CSVReader.ReadCSV(ReviewscsvPath,9);
        try {
            ReviewImporter.createReviewTable();
            ReviewImporter.importReviewData(reviewrows);
            ReviewImporter.importReviewRelated(reviewrows);
            ReviewImporter.dropReviewColumns();
        }catch (Exception e) {
            System.err.println("reviews 导入失败: " + e.getMessage());
            e.printStackTrace();
        }



    }
}
