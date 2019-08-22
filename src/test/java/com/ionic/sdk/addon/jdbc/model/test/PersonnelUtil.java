package com.ionic.sdk.addon.jdbc.model.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class PersonnelUtil {

    public static Object[] generate() {
        return new Object[]{generateFirstName(), generateLastName(), generateZipCode(), generateDepartment()};
    }

    /**
     * Test utility method.
     *
     * @return a random common first name
     */
    private static String generateFirstName() {
        // https://www.ssa.gov/oact/babynames/decades/century.html
        final List<String> firstNames = Arrays.asList(
                "James", "Mary", "John", "Patricia", "Robert",
                "Jennifer", "Michael", "Linda", "William", "Elizabeth",
                "David", "Barbara", "Richard", "Susan", "Joseph",
                "Jessica", "Thomas", "Sarah", "Charles", "Karen");
        Collections.shuffle(firstNames);
        return firstNames.iterator().next();
    }

    /**
     * Test utility method.
     *
     * @return a random common last name
     */
    private static String generateLastName() {
        // https://en.wikipedia.org/wiki/List_of_most_common_surnames_in_North_America#United_States_(American)
        final List<String> lastNames = Arrays.asList(
                "Smith", "Johnson", "Williams", "Brown", "Jones",
                "Miller", "Davis", "Garcia", "Rodriguez", "Wilson",
                "Martinez", "Anderson", "Taylor", "Thomas", "Hernandez",
                "Moore", "Martin", "Jackson", "Thompson", "White");
        Collections.shuffle(lastNames);
        return lastNames.iterator().next();
    }

    /**
     * Test utility method.
     *
     * @return a random 5-digit zip code
     */
    private static String generateZipCode() {
        final int zipCode = new Random().nextInt(100000);
        return Integer.toString(zipCode);
    }

    /**
     * Test utility method.
     *
     * @return a random department to be associated with a data record
     */
    private static String generateDepartment() {
        Collections.shuffle(DEPARTMENTS);
        final String department = DEPARTMENTS.iterator().next();
        //DEPARTMENTS.remove(department);  // only 3 departments
        return department;
    }

    private static final List<String> DEPARTMENTS = new ArrayList<String>(
            Arrays.asList("Engineering", "HR", "Marketing"));
}
