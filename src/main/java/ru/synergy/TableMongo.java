package ru.synergy;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static com.mongodb.client.model.Projections.*;

public class TableMongo {
    MongoDatabase myDB;
    MongoCollection<Document> employeesCollection;

    public TableMongo(MongoDatabase myDB, String tableName) {
        this.employeesCollection = myDB.getCollection("employees");
//        employeesCollection.deleteMany(new Document());
        insertDocument("Steven", "King", "SKING", "515.123.4567",
                LocalDate.of(1987, 6, 17), "AD_PRES", 24_000);
        insertDocument("Neena", "Kochhar", "NKOCHHAR", "515.123.4568",
                LocalDate.of(1987, 6, 18), "AD_PRES", 17_000);
        insertDocument("Lex", "De Haan", "LDEHAAN", "515.123.4569",
                LocalDate.of(1987, 6, 19), "AD_PRES", 17_000);
        insertDocument("Aleksander", "Hunold", "AHUNOLD", "590.423.4567",
                LocalDate.of(1987, 6, 20), "IT_PROG", 9_000);
        insertDocument("Bruce", "Ernst", "BERNST", "590.423.4568",
                LocalDate.of(1987, 6, 21), "IT_PROG", 6_000);
        insertDocument("David", "Austin", "DAUSTIN", "590.423.4569",
                LocalDate.of(1987, 6, 22), "IT_PROG", 4_800);
        insertDocument("Valli", "Pataballa", "VPATABAL", "590.423.4560",
                LocalDate.of(1987, 6, 23), "IT_PROG", 4_800);
    }

    public TableMongo(MongoCollection<Document> employeesCollection) {
        this.employeesCollection = employeesCollection;
    }

    private void insertDocument(String first_name, String last_name, String email, String phone_number,
                                LocalDate hire_date, String job_id, int salary) {
        Document employeeDocument = new Document(Map.of(
                "first_name", first_name,
                "last_name", last_name,
                "email", email,
                "phone_number", phone_number,
                "hire_date", hire_date,
                "job_id", job_id,
                "salary", salary));
        employeesCollection.insertOne(employeeDocument);
    }

    void printAllEmployees() {
        employeesCollection.find()
                .forEach((Consumer<Document>) System.out::println);
    }

    void printLastNameAndHireDateAllEmployees() {
        employeesCollection.find()
                .projection(fields(include("last_name", "hire_date"), excludeId()))
                        .forEach((Consumer<Document>) d -> System.out.println(d.getString("last_name") + ", "
                        + new SimpleDateFormat("yyyy-MM-dd").format(d.getDate("hire_date"))));
    }

    void printAllEmployeesBySalaryReduction() {
        employeesCollection.find()
                .sort(new Document("salary", -1))
                .forEach((Consumer<Document>) System.out::println);
    }

    void printAverageSalary() {
        AggregateIterable<Document> aggregate = employeesCollection.aggregate(Collections.singletonList(
                Document.parse("{ $group: { _id: null, averageSalary: { $avg: '$salary' } } }")
        ));
        Document result = aggregate.first();
        assert result != null;
        double averageSalary = result.getDouble("averageSalary");
        System.out.println("average salary = " + averageSalary);
    }

    void printNameAndPhoneNumberAllEmployees() {
        employeesCollection.find()
                .projection(fields(include("first_name", "phone_number"), excludeId()))
                .forEach((Consumer<? super Document>) System.out::println);
    }


}
