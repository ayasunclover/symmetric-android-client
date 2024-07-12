package org.jumpmind.symmetric.symmetricandroidclientdemo;

import android.annotation.SuppressLint;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;

import androidx.annotation.Nullable;

import org.jumpmind.symmetric.android.SQLiteOpenHelperRegistry;
import org.jumpmind.symmetric.android.SymmetricService;
import org.jumpmind.symmetric.common.ParameterConstants;

import java.util.Properties;



public class DbProviderServer extends ContentProvider {

    //TODO: Update REGISTRATION_URL with Sync URL of corp-000
    private final String REGISTRATION_URL = "";
    private final String SYNC_URL = "http://YOUR_CORP_IP_ADDRESS:31415/sync/corp-000";
    private final String NODE_ID = "000";
    private final String NODE_GROUP = "corp";
    private String ENGINE_NAME = "corp-000";

    final String SQL_CREATE_TABLE_ITEM = "CREATE TABLE IF NOT EXISTS ITEM(\n" +
            "    ITEM_ID INTEGER NOT NULL PRIMARY KEY ,\n" +
            "    NAME VARCHAR\n" +
            ");";

    final String SQL_CREATE_TABLE_ITEM_SELLING_PRICE = "CREATE TABLE IF NOT EXISTS ITEM_SELLING_PRICE(\n" +
            "    ITEM_ID INTEGER NOT NULL,\n" +
            "    STORE_ID VARCHAR NOT NULL,\n" +
            "    PRICE DECIMAL NOT NULL,\n" +
            "    COST DECIMAL,\n" +
            "    PRIMARY KEY (ITEM_ID, STORE_ID)\n" +
            ");";

    final String SQL_CREATE_TABLE_SALE_TRANSACTION = "CREATE TABLE IF NOT EXISTS SALE_TRANSACTION(\n" +
            "    TRAN_ID INTEGER NOT NULL PRIMARY KEY ,\n" +
            "    STORE_ID VARCHAR NOT NULL,\n" +
            "    WORKSTATION VARCHAR NOT NULL,\n" +
            "    DAY VARCHAR NOT NULL,\n" +
            "    SEQ INTEGER NOT NULL\n" +
            ");\n";

    final String SQL_CREATE_TABLE_SALE_RETURN_LINE_ITEM = "CREATE TABLE IF NOT EXISTS SALE_RETURN_LINE_ITEM(\n" +
            "    TRAN_ID INTEGER NOT NULL PRIMARY KEY ,\n" +
            "    ITEM_ID INTEGER NOT NULL,\n" +
            "    PRICE DECIMAL NOT NULL,\n" +
            "    QUANTITY INTEGER NOT NULL,\n" +
            "    RETURNED_QUANTITY INTEGER\n" +
            ");\n";

    public static final String DATABASE_NAME = "symmetric-demo.db";

    // Handle to a new DatabaseHelper.
    private DatabaseHelper mOpenHelper;

    /**
     * This class helps open, create, and upgrade the database file. Set to package visibility
     * for testing purposes.
     */
    static class DatabaseHelper extends SQLiteOpenHelper {

        DatabaseHelper(Context context) {

            // calls the super constructor, requesting the default cursor factory.
            super(context, DATABASE_NAME, null, 2);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
        }
        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            onCreate(db);
        }
    }

    /**
     * Initializes the provider by creating a new DatabaseHelper. onCreate() is called
     * automatically when Android creates the provider in response to a resolver request from a
     * client.
     */
    @SuppressLint("HardwareIds")
    @Override
    public boolean onCreate() {

        // Creates a new helper object. Note that the database itself isn't opened until
        // something tries to access it, and it's only created if it doesn't already exist.
        mOpenHelper = new DatabaseHelper(getContext());

        // Init the DB here
        SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        db.execSQL(SQL_CREATE_TABLE_ITEM);
        db.execSQL(SQL_CREATE_TABLE_ITEM_SELLING_PRICE);
        db.execSQL(SQL_CREATE_TABLE_SALE_TRANSACTION);
        db.execSQL(SQL_CREATE_TABLE_SALE_RETURN_LINE_ITEM);

        // Register the database helper, so it can be shared with the SymmetricService
        SQLiteOpenHelperRegistry.register(DATABASE_NAME, mOpenHelper);

        // SymmetricDS-Tabellen erstellen
        createSymmetricDSTables(db);

        // Zusätzliche SQL-Befehle zur Konfiguration von SymmetricDS ausführen
        configureSymmetricDS(db);

        Intent intent = new Intent(getContext(), SymmetricService.class);

        intent.putExtra(SymmetricService.INTENTKEY_SQLITEOPENHELPER_REGISTRY_KEY, DATABASE_NAME);
        intent.putExtra(SymmetricService.INTENTKEY_REGISTRATION_URL, REGISTRATION_URL);
        intent.putExtra(SymmetricService.INTENTKEY_EXTERNAL_ID, NODE_ID);
        intent.putExtra(SymmetricService.INTENTKEY_NODE_GROUP_ID, NODE_GROUP);
        intent.putExtra(SymmetricService.INTENTKEY_START_IN_BACKGROUND, true);


        // TODO: Update properties with the desired Symmetric parameters (e.g. File Sync parameters)
        Properties properties = new Properties();

        // Workaround for SymmetricDS 3.9 versions with incompatible parameter defaults
        properties.put(ParameterConstants.STREAM_TO_FILE_ENABLED, "false");
        properties.put(ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB, "false");

        properties.put(ParameterConstants.AUTO_REGISTER_ENABLED, "true");
        properties.put(ParameterConstants.ENGINE_NAME, ENGINE_NAME);
        properties.put(ParameterConstants.SYNC_URL, SYNC_URL);


        //properties.put(ParameterConstants.FILE_SYNC_ENABLE, "true");
        //properties.put("start.file.sync.tracker.job", "true");
        //properties.put("start.file.sync.push.job", "true");
        //properties.put("start.file.sync.pull.job", "true");
        //properties.put("job.file.sync.pull.period.time.ms", "10000");

        intent.putExtra(SymmetricService.INTENTKEY_PROPERTIES, properties);

        getContext().startService(intent);

        // Assumes that any failures will be reported by a thrown exception.
        return true;
    }

    @Nullable
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        return null;
    }

    public String getType(Uri uri) {

        throw new IllegalArgumentException("Unknown URI " + uri);

    }

    @Nullable
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        return null;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        return 0;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    private void createSymmetricDSTables(SQLiteDatabase db) {
        String[] symmetricDSTableCreationSQL = new String[]{
                "CREATE TABLE IF NOT EXISTS sym_trigger_router (" +
                        "trigger_id VARCHAR(50) NOT NULL," +
                        "router_id VARCHAR(50) NOT NULL," +
                        "enabled INTEGER DEFAULT (1) NOT NULL," +
                        "initial_load_order INTEGER DEFAULT (1) NOT NULL," +
                        "initial_load_select VARCHAR," +
                        "initial_load_delete_stmt VARCHAR," +
                        "ping_back_enabled INTEGER DEFAULT (0) NOT NULL," +
                        "create_time TIMESTAMP NOT NULL," +
                        "last_update_by VARCHAR," +
                        "last_update_time TIMESTAMP NOT NULL," +
                        "description VARCHAR," +
                        "PRIMARY KEY (trigger_id, router_id)," +
                        "FOREIGN KEY (trigger_id) REFERENCES sym_trigger (trigger_id)," +
                        "FOREIGN KEY (router_id) REFERENCES sym_router (router_id)" +
                        ");",
                "CREATE TABLE IF NOT EXISTS sym_trigger (" +
                        "trigger_id VARCHAR(50) NOT NULL PRIMARY KEY," +
                        "source_catalog_name VARCHAR," +
                        "source_schema_name VARCHAR," +
                        "source_table_name VARCHAR NOT NULL," +
                        "channel_id VARCHAR NOT NULL," +
                        "reload_channel_id VARCHAR DEFAULT ('reload') NOT NULL," +
                        "sync_on_update INTEGER DEFAULT (1) NOT NULL," +
                        "sync_on_insert INTEGER DEFAULT (1) NOT NULL," +
                        "sync_on_delete INTEGER DEFAULT (1) NOT NULL," +
                        "sync_on_incoming_batch INTEGER DEFAULT (0) NOT NULL," +
                        "name_for_update_trigger VARCHAR," +
                        "name_for_insert_trigger VARCHAR," +
                        "name_for_delete_trigger VARCHAR," +
                        "sync_on_update_condition VARCHAR," +
                        "sync_on_insert_condition VARCHAR," +
                        "sync_on_delete_condition VARCHAR," +
                        "custom_before_update_text VARCHAR," +
                        "custom_before_insert_text VARCHAR," +
                        "custom_before_delete_text VARCHAR," +
                        "custom_on_update_text VARCHAR," +
                        "custom_on_insert_text VARCHAR," +
                        "custom_on_delete_text VARCHAR," +
                        "external_select VARCHAR," +
                        "tx_id_expression VARCHAR," +
                        "channel_expression VARCHAR," +
                        "excluded_column_names VARCHAR," +
                        "included_column_names VARCHAR," +
                        "sync_key_names VARCHAR," +
                        "use_stream_lobs INTEGER DEFAULT (0) NOT NULL," +
                        "use_capture_lobs INTEGER DEFAULT (0) NOT NULL," +
                        "use_capture_old_data INTEGER DEFAULT (1) NOT NULL," +
                        "use_handle_key_updates INTEGER DEFAULT (1) NOT NULL," +
                        "stream_row INTEGER DEFAULT (0) NOT NULL," +
                        "create_time TIMESTAMP NOT NULL," +
                        "last_update_by VARCHAR," +
                        "last_update_time TIMESTAMP NOT NULL," +
                        "description VARCHAR," +
                        "FOREIGN KEY (channel_id) REFERENCES sym_channel (channel_id)," +
                        "FOREIGN KEY (reload_channel_id) REFERENCES sym_channel (channel_id)" +
                        ");",
                "CREATE TABLE IF NOT EXISTS sym_router (" +
                        "router_id VARCHAR(50) NOT NULL PRIMARY KEY," +
                        "target_catalog_name VARCHAR," +
                        "target_schema_name VARCHAR," +
                        "target_table_name VARCHAR," +
                        "source_node_group_id VARCHAR NOT NULL," +
                        "target_node_group_id VARCHAR NOT NULL," +
                        "router_type VARCHAR DEFAULT ('default') NOT NULL," +
                        "router_expression VARCHAR," +
                        "sync_on_update INTEGER DEFAULT (1) NOT NULL," +
                        "sync_on_insert INTEGER DEFAULT (1) NOT NULL," +
                        "sync_on_delete INTEGER DEFAULT (1) NOT NULL," +
                        "use_source_catalog_schema INTEGER DEFAULT (1) NOT NULL," +
                        "create_time TIMESTAMP NOT NULL," +
                        "last_update_by VARCHAR," +
                        "last_update_time TIMESTAMP NOT NULL," +
                        "description VARCHAR," +
                        "FOREIGN KEY (source_node_group_id, target_node_group_id) REFERENCES sym_node_group_link (source_node_group_id, target_node_group_id)" +
                        ");",
                "CREATE TABLE IF NOT EXISTS sym_channel (" +
                        "channel_id VARCHAR(50) NOT NULL PRIMARY KEY," +
                        "processing_order INTEGER DEFAULT 1 NOT NULL," +
                        "max_batch_size INTEGER DEFAULT 1000 NOT NULL," +
                        "max_batch_to_send INTEGER DEFAULT 60 NOT NULL," +
                        "max_data_to_route INTEGER DEFAULT 100000 NOT NULL," +
                        "extract_period_millis INTEGER DEFAULT 0 NOT NULL," +
                        "enabled INTEGER DEFAULT 1 NOT NULL," +
                        "use_old_data_to_route INTEGER DEFAULT 1 NOT NULL," +
                        "use_row_data_to_route INTEGER DEFAULT 1 NOT NULL," +
                        "use_pk_data_to_route INTEGER DEFAULT 1 NOT NULL," +
                        "reload_flag INTEGER DEFAULT 0 NOT NULL," +
                        "file_sync_flag INTEGER DEFAULT 0 NOT NULL," +
                        "contains_big_lob INTEGER DEFAULT 0 NOT NULL," +
                        "batch_algorithm VARCHAR DEFAULT 'default' NOT NULL," +
                        "data_loader_type VARCHAR DEFAULT 'default' NOT NULL," +
                        "description VARCHAR(255)," +
                        "queue VARCHAR DEFAULT 'default' NOT NULL," +
                        "max_network_kbps DECIMAL(10,3) DEFAULT 0.000 NOT NULL," +
                        "data_event_action VARCHAR," +
                        "create_time TIMESTAMP," +
                        "last_update_by VARCHAR," +
                        "last_update_time TIMESTAMP" +
                        ");",
                "CREATE TABLE IF NOT EXISTS sym_node_group_link (" +
                        "source_node_group_id VARCHAR(50) NOT NULL," +
                        "target_node_group_id VARCHAR(50) NOT NULL," +
                        "data_event_action VARCHAR DEFAULT ('W') NOT NULL," +
                        "sync_config_enabled INTEGER DEFAULT (1) NOT NULL," +
                        "is_reversible INTEGER DEFAULT (0) NOT NULL," +
                        "create_time TIMESTAMP," +
                        "last_update_by VARCHAR," +
                        "last_update_time TIMESTAMP," +
                        "PRIMARY KEY (source_node_group_id, target_node_group_id)," +
                        "FOREIGN KEY (source_node_group_id) REFERENCES sym_node_group (node_group_id)," +
                        "FOREIGN KEY (target_node_group_id) REFERENCES sym_node_group (node_group_id)" +
                        ");",
                "CREATE TABLE IF NOT EXISTS sym_node_group (" +
                        "node_group_id VARCHAR(50) NOT NULL PRIMARY KEY," +
                        "description VARCHAR(255)," +
                        "create_time TIMESTAMP," +
                        "last_update_by VARCHAR," +
                        "last_update_time TIMESTAMP" +
                        ");",
                "CREATE TABLE IF NOT EXISTS sym_node_host (" +
                        "node_id VARCHAR(50) NOT NULL," +
                        "host_name VARCHAR(255) NOT NULL," +
                        "instance_id VARCHAR," +
                        "ip_address VARCHAR," +
                        "os_user VARCHAR," +
                        "os_name VARCHAR," +
                        "os_arch VARCHAR," +
                        "os_version VARCHAR," +
                        "available_processors INTEGER DEFAULT 0," +
                        "free_memory_bytes INTEGER DEFAULT 0," +
                        "total_memory_bytes INTEGER DEFAULT 0," +
                        "max_memory_bytes INTEGER DEFAULT 0," +
                        "java_version VARCHAR," +
                        "java_vendor VARCHAR," +
                        "jdbc_version VARCHAR," +
                        "symmetric_version VARCHAR," +
                        "timezone_offset VARCHAR," +
                        "heartbeat_time TIMESTAMP," +
                        "last_restart_time TIMESTAMP NOT NULL," +
                        "create_time TIMESTAMP NOT NULL," +
                        "PRIMARY KEY (node_id, host_name)" +
                        ");",
                "CREATE TABLE IF NOT EXISTS sym_node_identity (" +
                        "node_id VARCHAR(50) NOT NULL PRIMARY KEY," +
                        "FOREIGN KEY (node_id) REFERENCES sym_node (node_id)" +
                        ");",
                "CREATE TABLE IF NOT EXISTS sym_node_security (" +
                        "node_id VARCHAR NOT NULL PRIMARY KEY," +
                        "node_password VARCHAR NOT NULL," +
                        "registration_enabled INTEGER DEFAULT 0," +
                        "registration_time TIMESTAMP," +
                        "registration_not_before TIMESTAMP," +
                        "registration_not_after TIMESTAMP," +
                        "initial_load_enabled INTEGER DEFAULT 0," +
                        "initial_load_time TIMESTAMP," +
                        "initial_load_end_time TIMESTAMP," +
                        "initial_load_id INTEGER," +
                        "initial_load_create_by VARCHAR," +
                        "rev_initial_load_enabled INTEGER DEFAULT 0," +
                        "rev_initial_load_time TIMESTAMP," +
                        "rev_initial_load_id INTEGER," +
                        "rev_initial_load_create_by VARCHAR," +
                        "failed_logins INTEGER DEFAULT 0," +
                        "created_at_node_id VARCHAR," +
                        "FOREIGN KEY (node_id) REFERENCES sym_node (node_id)" +
                        ");",
                "CREATE TABLE IF NOT EXISTS sym_node (" +
                        "node_id VARCHAR(50) NOT NULL PRIMARY KEY," +
                        "node_group_id VARCHAR(50) NOT NULL," +
                        "external_id VARCHAR(50) NOT NULL," +
                        "sync_enabled INTEGER DEFAULT 0," +
                        "sync_url VARCHAR," +
                        "schema_version VARCHAR," +
                        "symmetric_version VARCHAR," +
                        "config_version VARCHAR," +
                        "database_type VARCHAR," +
                        "database_version VARCHAR," +
                        "database_name VARCHAR," +
                        "batch_to_send_count INTEGER DEFAULT 0," +
                        "batch_in_error_count INTEGER DEFAULT 0," +
                        "created_at_node_id VARCHAR," +
                        "deployment_type VARCHAR," +
                        "deployment_sub_type VARCHAR" +
                        ");"
        };

        for (String sql : symmetricDSTableCreationSQL) {
            db.execSQL(sql);
        }
    }

    private void configureSymmetricDS(SQLiteDatabase db) {
        String[] sqlStatements = new String[] {
                "delete from sym_trigger_router;",
                "delete from sym_trigger;",
                "delete from sym_router;",
                "delete from sym_channel where channel_id in ('sale_transaction', 'item');",
                "delete from sym_node_group_link;",
                "delete from sym_node_group;",
                "delete from sym_node_host;",
                "delete from sym_node_identity;",
                "delete from sym_node_security;",
                "delete from sym_node;",

                "insert into sym_channel (channel_id, processing_order, max_batch_size, enabled, description) values('sale_transaction', 1, 100000, 1, 'sale_transactional data from register and back office');",
                "insert into sym_channel (channel_id, processing_order, max_batch_size, enabled, description) values('item', 1, 100000, 1, 'Item and pricing data');",
                "insert into sym_node_group (node_group_id) values ('corp');",
                "insert into sym_node_group (node_group_id) values ('store');",
                "insert into sym_node_group_link (source_node_group_id, target_node_group_id, data_event_action) values ('corp', 'store', 'W');",
                "insert into sym_node_group_link (source_node_group_id, target_node_group_id, data_event_action) values ('store', 'corp', 'P');",

                "insert into sym_trigger (trigger_id, source_table_name, channel_id, last_update_time, create_time) values('item_selling_price', 'item_selling_price', 'item', current_timestamp, current_timestamp);",
                "insert into sym_trigger (trigger_id, source_table_name, channel_id, last_update_time, create_time) values('item', 'item', 'item', current_timestamp, current_timestamp);",
                "insert into sym_trigger (trigger_id, source_table_name, channel_id, last_update_time, create_time) values('sale_transaction', 'sale_transaction', 'sale_transaction', current_timestamp, current_timestamp);",
                "insert into sym_trigger (trigger_id, source_table_name, channel_id, last_update_time, create_time) values('sale_return_line_item', 'sale_return_line_item', 'sale_transaction', current_timestamp, current_timestamp);",

                "insert into sym_trigger (trigger_id, source_table_name, channel_id, sync_on_insert, sync_on_update, sync_on_delete, last_update_time, create_time) values('sale_transaction_corp', 'sale_transaction', 'sale_transaction', 0, 0, 0, current_timestamp, current_timestamp);",
                "insert into sym_trigger (trigger_id, source_table_name, channel_id, sync_on_insert, sync_on_update, sync_on_delete, last_update_time, create_time) values('sale_return_line_item_corp', 'sale_return_line_item', 'sale_transaction', 0, 0, 0, current_timestamp, current_timestamp);",

                "insert into sym_router (router_id, source_node_group_id, target_node_group_id, router_type, create_time, last_update_time) values('corp_2_store', 'corp', 'store', 'default', current_timestamp, current_timestamp);",
                "insert into sym_router (router_id, source_node_group_id, target_node_group_id, router_type, create_time, last_update_time) values('store_2_corp', 'store', 'corp', 'default', current_timestamp, current_timestamp);",
                "insert into sym_router (router_id, source_node_group_id, target_node_group_id, router_type, router_expression, create_time, last_update_time) values('corp_2_one_store', 'corp', 'store', 'column', 'STORE_ID=:EXTERNAL_ID or OLD_STORE_ID=:EXTERNAL_ID', current_timestamp, current_timestamp);",

                "insert into sym_trigger_router (trigger_id, router_id, initial_load_order, last_update_time, create_time) values('item', 'corp_2_store', 100, current_timestamp, current_timestamp);",
                "insert into sym_trigger_router (trigger_id, router_id, initial_load_order, initial_load_select, last_update_time, create_time) values('item_selling_price', 'corp_2_one_store', 100, 'store_id=''$(externalId)''', current_timestamp, current_timestamp);",
                "insert into sym_trigger_router (trigger_id, router_id, initial_load_order, last_update_time, create_time) values('sale_transaction', 'store_2_corp', 200, current_timestamp, current_timestamp);",
                "insert into sym_trigger_router (trigger_id, router_id, initial_load_order, last_update_time, create_time) values('sale_return_line_item', 'store_2_corp', 200, current_timestamp, current_timestamp);",
                "insert into sym_trigger_router (trigger_id, router_id, initial_load_order, last_update_time, create_time) values('sale_transaction_corp', 'corp_2_store', 200, current_timestamp, current_timestamp);",
                "insert into sym_trigger_router (trigger_id, router_id, initial_load_order, last_update_time, create_time) values('sale_return_line_item_corp', 'corp_2_store', 200, current_timestamp, current_timestamp);"
        };

        for (String sql : sqlStatements) {
            db.execSQL(sql);
        }
    }
}
