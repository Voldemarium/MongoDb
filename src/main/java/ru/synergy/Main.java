package ru.synergy;

import com.mongodb.client.MongoClients;

public class Main {
    public static void main(String[] args) {
        try (var mongoClient = MongoClients.create()) {
            //  Создание и получение базы данных
            var myDB = mongoClient.getDatabase("myMongoDB");

            //  Создание и получение таблицы (коллекции)
            //            TableMongo tableMongo = new TableMongo(myDB,"employees");
            TableMongo tableMongo = new TableMongo(myDB.getCollection("employees"));

//  1. Напишите запрос MongoDB для отображения всех данных из представленной таблицы
            tableMongo.printAllEmployees();
            System.out.println();

//  2. Напишите запрос MongoDB для отображения ФИО и даты рождения всех лиц из представленной таблицы
            tableMongo.printLastNameAndHireDateAllEmployees();
            System.out.println();
//  3. Напишите запрос MongoDB для отображения всех работников сортирую их в порядке уменьшения заработной платы
            tableMongo.printAllEmployeesBySalaryReduction();
            System.out.println();

//  4. Напишите запрос MongoDB для отображения средней зарплаты всех работников
            tableMongo.printAverageSalary();
            System.out.println();

//  5. Напишите запрос MongoDB для отображения только имени и номера телефона сотрудников из представленной таблицы
            tableMongo.printNameAndPhoneNumberAllEmployees();
        }
    }
}