package org.example.beamdemo;

import com.github.javafaker.Faker;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.storage.v1alpha2.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.Arrays;

@Slf4j
public class Main {

    private final static String TABLE_USER_FIELD_ID         = "ID";
    private final static String TABLE_USER_FIELD_FIRSTNAME  = "Firstname";
    private final static String TABLE_USER_FIELD_LASTNAME   = "Lastname";
    private final static String TABLE_USER_FIELD_EMAIL      = "Email";
    private final static String TABLE_USER_FIELD_PHONE      = "Phone";

    private static String project = "devproject-b6f97";
    private static String dataset = "TestDB";
    private static String tableUser = "User";

    public static void main(String[] args) {

        log.info("Application started");

        // Generate fake Users
        ArrayList<User> users = generateFakeUsers(100);

        // Create beam pipeline
        // https://beam.apache.org/documentation/programming-guide/#configuring-pipeline-options
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setTempLocation("gs://devproject-b6f97.appspot.com");
        Pipeline pipeline = Pipeline.create(options);


        // https://beam.apache.org/documentation/programming-guide/#creating-pcollection-in-memory
        PCollection<User> userPCollection = pipeline.apply(Create.of(users)).setCoder(SerializableCoder.of(User.class));

        // Convert user pcollection to tablerow pcollection
        PCollection<TableRow> rowCollection = userPCollection.apply(
                "formatData",
                ParDo.of(new DoFn<User, TableRow>() {
                    @ProcessElement
                    public void processElement(@Element User user, OutputReceiver<TableRow> out)
                    {
                        TableRow row = new TableRow()
                                .set("ID", user.id)
                                .set("Firstname", user.firstName)
                                .set("Lastname", user.lastName)
                                .set("Email", user.email)
                                .set("Phone", user.phone);
                        out.output(row);
                    }
                }));

        // Get table schema
        TableSchema schema = getUserTableSchema();

        // Write to pcollection to table
        writeToTable(project, dataset, tableUser, schema, rowCollection);

        // Execute pipeline
        pipeline.run().waitUntilFinish();

    }


    // Create test users
    private static ArrayList<User> generateFakeUsers(int count) {
        log.info("");

        ArrayList<User> list = new ArrayList<>();

        for(int i = 0; i < count; i++) {
            Faker faker = new Faker();

            String firstName = faker.name().firstName();
            String lastName = faker.name().lastName();
            String email = faker.internet().emailAddress(firstName + "." + lastName);
            String phone = faker.phoneNumber().cellPhone();

            User user = User.builder()
                    .id(i)
                    .firstName(firstName)
                    .lastName(lastName)
                    .email(email)
                    .phone(phone)
                    .build();

            // log user
            log.info("Created User: " + user.toString());

            list.add(user);
        }

        return list;
    }

    static TableSchema getUserTableSchema() {
        TableSchema schema =
                new TableSchema()
                        .setFields(
                                Arrays.asList(
                                        new TableFieldSchema()
                                                .setName("ID")
                                                .setType("INTEGER")
                                                .setMode("REQUIRED"),
                                        new TableFieldSchema()
                                                .setName("Firstname")
                                                .setType("STRING")
                                                .setMode("REQUIRED"),
                                        new TableFieldSchema()
                                                .setName("Lastname")
                                                .setType("STRING")
                                                .setMode("REQUIRED"),
                                        new TableFieldSchema()
                                                .setName("Email")
                                                .setType("STRING")
                                                .setMode("REQUIRED"),
                                        new TableFieldSchema()
                                                .setName("Phone")
                                                .setType("STRING")
                                                .setMode("NULLABLE")
                                        )
                        );
        return schema;
    }

    // https://beam.apache.org/documentation/io/built-in/google-bigquery/#writing-to-a-table
    public static void writeToTable(
            String project,
            String dataset,
            String table,
            TableSchema schema,
            PCollection<TableRow> rows) {

        rows.apply(
                "Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.%s", project, dataset, table))
                        .withSchema(schema)
                        // For CreateDisposition:
                        // - CREATE_IF_NEEDED (default): creates the table if it doesn't exist, a schema is
                        // required
                        // - CREATE_NEVER: raises an error if the table doesn't exist, a schema is not needed
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        // For WriteDisposition:
                        // - WRITE_EMPTY (default): raises an error if the table is not empty
                        // - WRITE_APPEND: appends new rows to existing rows
                        // - WRITE_TRUNCATE: deletes the existing rows before writing
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    }

}

