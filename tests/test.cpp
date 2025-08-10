#include "ldb.hpp"
#include <iostream>
#include <vector>
#include <cassert>

void print_results(const std::string& title, const std::vector<ldb::Row>& rows) {
    std::cout << "--- " << title << " ---\n";
    if (rows.empty()) {
        std::cout << "(No results)\n";
    } else {
        for (const auto& row : rows) {
            std::cout << row << "\n";
        }
    }
    std::cout << std::endl;
}

void print_joined_results(const std::string& title, const std::vector<ldb::JoinedRow>& rows) {
    std::cout << "--- " << title << " ---\n";
    if (rows.empty()) {
        std::cout << "(No results)\n";
    } else {
        for (const auto& j_row : rows) {
            std::cout << "Left: " << j_row.left;
            if (j_row.right) {
                std::cout << ", Right: " << *j_row.right;
            } else {
                std::cout << ", Right: NULL";
            }
            std::cout << "\n";
        }
    }
    std::cout << std::endl;
}

int main() {
    try {
        ldb::LightDB db;
        std::cout << "Database initialised.\n\n";

        std::cout << "Defining schemas and creating tables...\n";

        ldb::Schema manufacturers_schema("id");
        manufacturers_schema.add_field<std::string>("name")
                            .add_field<std::string>("country")
                            .add_index("name");

        ldb::Schema models_schema("id");
        models_schema.add_field<std::string>("name")
                     .add_field<int>("year")
                     .add_field<bool>("is_electric")
                     .add_field<double>("price", true)
                     .add_field<int>("manufacturer_id")
                     .add_foreign_key("manufacturer_id", "manufacturers", "id", ldb::OnDeleteAction::Restrict)
                     .add_index("year");

         ldb::Schema features_schema("id");
         features_schema.add_field<std::string>("feature_name")
                        .add_field<int>("model_id")
                        .add_foreign_key("model_id", "models", "id", ldb::OnDeleteAction::Cascade);


        db.create_table("manufacturers", manufacturers_schema);
        db.create_table("models", models_schema);
        db.create_table("features", features_schema);

        std::cout << "Tables 'manufacturers', 'models', and 'features' created.\n\n";

        std::cout << "Inserting data...\n";
        ldb::Table* manufacturers = db.get_table("manufacturers");
        ldb::Table* models = db.get_table("models");

        auto manu_ford_id = manufacturers->create_row(db).set("id", 1).set("name", "Ford").set("country", "USA").insert();
        auto manu_toyota_id = manufacturers->create_row(db).set("id", 2).set("name", "Toyota").set("country", "Japan").insert();
        auto manu_tesla_id = manufacturers->create_row(db).set("id", 3).set("name", "Tesla").set("country", "USA").insert();
        
        assert(manu_ford_id == 1 && manu_toyota_id == 2 && manu_tesla_id == 3);

        models->create_row(db).set("id", 101).set("name", "Mustang").set("year", 2022).set("is_electric", false).set("price", 45000.0).set("manufacturer_id", manu_ford_id).insert();
        models->create_row(db).set("id", 102).set("name", "Camry").set("year", 2023).set("is_electric", false).set("price", 28000.0).set("manufacturer_id", manu_toyota_id).insert();
        auto model3_id = models->create_row(db).set("id", 103).set("name", "Model 3").set("year", 2023).set("is_electric", true).set("price", 40000.0).set("manufacturer_id", manu_tesla_id).insert();
        auto modelS_id = models->create_row(db).set("id", 104).set("name", "Model S").set("year", 2022).set("is_electric", true).set("price", 80000.0).set("manufacturer_id", manu_tesla_id).insert();
        models->create_row(db).set("id", 105).set("name", "F-150 Lightning").set("year", 2023).set("is_electric", true).set("price", 75000.0).set("manufacturer_id", manu_ford_id).insert();

        std::cout << "Data inserted successfully.\n\n";

        std::cout << "--- Running Basic Queries ---\n";

        auto model_s = models->get(modelS_id);
        if (model_s) {
            std::cout << "Get Model S by ID: " << *model_s << "\n\n";
        }
        
        auto electric_cars = models->query(db).where("is_electric").equals(true).execute();
        print_results("All Electric Cars", electric_cars);

        auto tesla_cars = models->query(db).where("manufacturer_id").equals(manu_tesla_id).execute();
        print_results("All Tesla Cars", tesla_cars);

        auto new_cars_sorted = models->query(db).where("year").equals(2023).order_by("price", ldb::SortOrder::Descending).execute();
        print_results("Cars from 2023 (Price High to Low)", new_cars_sorted);

        std::cout << "--- Running Aggregation Queries ---\n";
        size_t electric_count = models->query(db).where("is_electric").equals(true).count();
        std::cout << "Number of electric car models: " << electric_count << "\n";
        assert(electric_count == 3);

        auto total_price = models->query(db).sum("price");
        if (total_price) {
            std::cout << "Sum of all car prices: " << std::any_cast<double>(*total_price) << "\n";
        }

        auto avg_price_electric = models->query(db).where("is_electric").equals(true).avg("price");
         if (avg_price_electric) {
            std::cout << "Average price of electric cars: " << std::any_cast<double>(*avg_price_electric) << "\n";
        }

        auto cheapest_car_price = models->query(db).min("price");
        std::cout << "Cheapest car price: " << std::any_cast<double>(*cheapest_car_price) << "\n";
        assert(std::any_cast<double>(*cheapest_car_price) == 28000.0);

        auto most_expensive_car_price = models->query(db).max("price");
        std::cout << "Most expensive car price: " << std::any_cast<double>(*most_expensive_car_price) << "\n\n";
        assert(std::any_cast<double>(*most_expensive_car_price) == 80000.0);

        std::cout << "--- Updating and Deleting ---\n";
        std::cout << "Giving the Model 3 a price drop...\n";
        int updated_count = models->query(db).where("id").equals(model3_id).update({{"price", 38500.0}});
        assert(updated_count == 1);
        
        auto updated_model3 = models->get(model3_id);
        std::cout << "Updated Model 3: " << *updated_model3 << "\n";

        std::cout << "Discontinuing the Mustang...\n";
        int removed_count = models->query(db).where("name").equals("Mustang").remove();
        assert(removed_count == 1);
        print_results("Models after deleting Mustang", models->query(db).execute());
        
        auto joined_data = db.join("models", "manufacturers", "manufacturer_id", "id");
        print_joined_results("Inner Join: Models and their Manufacturers", joined_data);

        std::cout << "--- Testing Foreign Key Constraints ---\n";
        std::cout << "Attempting to delete 'Tesla', which still has models (should fail)...\n";
        try {
            manufacturers->remove(manu_tesla_id, db);
        } catch (const ldb::LightDBException& e) {
            std::cout << "Caught expected exception: " << e.what() << "\n\n";
        }

        std::cout << "Testing cascade delete...\n";
        ldb::Table* features = db.get_table("features");
        features->create_row(db).set("feature_name", "Yoke Steering").set("model_id", modelS_id).insert();
        std::cout << "Features count before delete: " << features->query(db).count() << "\n";
        assert(features->query(db).count() == 1);
        
        std::cout << "Deleting Model S...\n";
        models->remove(modelS_id, db);
        
        std::cout << "Features count after cascade delete: " << features->query(db).count() << "\n\n";
        assert(features->query(db).count() == 0);

        std::cout << "--- Testing Transaction ---\n";
        ldb::Transaction txn(db);
        
        txn.execute([&](ldb::LightDB& tx_db) {
            std::cout << "Inside transaction: Adding 'Porsche' and a 'Taycan' model.\n";
            ldb::Table* tx_manufacturers = tx_db.get_table("manufacturers");
            ldb::Table* tx_models = tx_db.get_table("models");

            auto porsche_id = tx_manufacturers->create_row(tx_db).set("id", 4).set("name", "Porsche").set("country", "Germany").insert();
            tx_models->create_row(tx_db).set("id", 106).set("name", "Taycan").set("year", 2023).set("is_electric", true).set("price", 90000.0).set("manufacturer_id", porsche_id).insert();
            
            assert(tx_db.get_table("manufacturers")->query(tx_db).count() == 4);
        });

        std::cout << "Transaction committed.\n";
        print_results("Manufacturers after transaction", manufacturers->query(db).execute());
        assert(manufacturers->query(db).count() == 4);

        const std::string save_path = "cars.json";
        std::cout << "\n--- Saving and Loading ---\n";
        std::cout << "Saving database to '" << save_path << "'...\n";
        db.save(save_path);
        
        std::cout << "Loading database from '" << save_path << "' into a new DB instance...\n";
        ldb::LightDB loaded_db = ldb::LightDB::load(save_path);
        
        std::cout << "Verifying loaded data by querying for Porsche...\n";
        auto loaded_manufacturers = loaded_db.get_table("manufacturers");
        auto porsche_query = loaded_manufacturers->query(loaded_db).where("name").equals("Porsche").execute();
        
        print_results("Porsche from loaded DB", porsche_query);
        assert(!porsche_query.empty());
        assert(std::any_cast<std::string>(porsche_query.front().at("country")) == "Germany");

        std::cout << "\nAll tests passed successfully!\n";

    } catch (const ldb::LightDBException& e) {
        std::cerr << "An unexpected error occurred: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}