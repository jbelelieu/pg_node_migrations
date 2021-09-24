import * as pg from "pg"
import SQL from "sql-template-strings"
import {runCreateQuery} from "./create"
import {loadMigrationFiles} from "./files-loader"
import {runMigration} from "./run-migration"
import {
  BasicPgClient,
  Config,
  Logger,
  MigrateDBConfig,
  Migration,
  MigrationError,
  Options
} from "./types"
import {validateMigrationHashes} from "./validation"
import {withConnection} from "./with-connection"
import {withAdvisoryLock} from "./with-lock"

/**
 * Run the migrations.
 *
 * If `dbConfig.ensureDatabaseExists` is true then `dbConfig.database` will be created if it
 * does not exist.
 *
 * @param dbConfig Details about how to connect to the database
 * @param migrationsDirectory Directory containing the SQL migration files
 * @param config Extra configuration
 * @returns Details about the migrations which were run
 */
export async function migrate(
  dbConfig: MigrateDBConfig,
  migrationsDirectory: string,
  config: Config = {},
): Promise<Array<Migration>> {
  const log =
    config.logger != null
      ? config.logger
      : () => {
          //
        }

  const options = {
    tableName: "tableName" in config && config.tableName !== undefined ? config.tableName : 'migrations',
    schemaName: "schemaName" in config && config.schemaName !== undefined ? config.schemaName : 'public'
  }
      
  if (dbConfig == null) {
    throw new Error("No config object")
  }

  if (typeof migrationsDirectory !== "string") {
    throw new Error("Must pass migrations directory as a string")
  }
  
  const intendedMigrations = await loadMigrationFiles(migrationsDirectory, log)

  if ("client" in dbConfig) {
    // we have been given a client to use, it should already be connected
    return withAdvisoryLock(
      log,
      runMigrations(intendedMigrations, log, options),
    )(dbConfig.client)
  }

  if (
    typeof dbConfig.database !== "string" ||
    typeof dbConfig.user !== "string" ||
    typeof dbConfig.password !== "string" ||
    typeof dbConfig.host !== "string" ||
    typeof dbConfig.port !== "number"
  ) {
    throw new Error("Database config problem")
  }

  if (dbConfig.ensureDatabaseExists === true) {
    // Check whether database exists
    const {user, password, host, port} = dbConfig
    const client = new pg.Client({
      database:
        dbConfig.defaultDatabase != null
          ? dbConfig.defaultDatabase
          : "postgres",
      user,
      password,
      host,
      port,
    })

    const runWith = withConnection(log, async (connectedClient) => {
      const result = await connectedClient.query({
        text: "SELECT 1 FROM pg_database WHERE datname=$1",
        values: [dbConfig.database],
      })
      if (result.rowCount !== 1) {
        await runCreateQuery(dbConfig.database, log)(connectedClient)
      }
    })

    await runWith(client)
  }
  {
    const client = new pg.Client(dbConfig)
    client.on("error", (err: any) => {
      log(`pg client emitted an error: ${err.message}`)
    })

    const runWith = withConnection(
      log,
      withAdvisoryLock(log, runMigrations(intendedMigrations, log, options)),
    )

    return runWith(client)
  }
}

function runMigrations(intendedMigrations: Array<Migration>, log: Logger, options: Options) {
  return async (client: BasicPgClient) => {
    try {


      log(`Starting migrations against schema ${options.schemaName}`)

      const appliedMigrations = await fetchAppliedMigrationFromDB(
        options.tableName,
        options.schemaName,
        client,
        log,
      )

      validateMigrationHashes(intendedMigrations, appliedMigrations)

      const migrationsToRun = filterMigrations(
        intendedMigrations,
        appliedMigrations,
      )
      const completedMigrations = []

      for (const migration of migrationsToRun) {
        log(`Starting migration: ${migration.id} ${migration.name}`)
        const result = await runMigration(
          options.tableName,
          options.schemaName,
          client,
          log,
        )(migration)
        log(`Finished migration: ${migration.id} ${migration.name}`)
        completedMigrations.push(result)
      }

      logResult(completedMigrations, log)

      log("Finished migrations")

      return completedMigrations
    } catch (e: any) {
      const error: MigrationError = new Error(
        `Migration failed. Reason: ${e.message}`,
      )
      error.cause = e.message
      throw error
    }
  }
}

/** Queries the database for migrations table and retrieve it rows if exists */
async function fetchAppliedMigrationFromDB(
  migrationTableName: string,
  migrationSchemaName: string,
  client: BasicPgClient,
  log: Logger,
) {
  let appliedMigrations = []

  if (await doesTableExist(client, migrationTableName, migrationSchemaName)) {
    log(
      `Migrations table with name '${migrationSchemaName}.${migrationTableName}' exists, filtering not applied migrations.`,
    )

    const {rows} = await client.query(
      `SELECT * FROM "${migrationSchemaName}"."${migrationTableName}" ORDER BY id`,
    )
    
    appliedMigrations = rows
  } else {
    await client.query(`
        CREATE TABLE IF NOT EXISTS "${migrationSchemaName}"."${migrationTableName}" (
          id integer PRIMARY KEY,
          name varchar(100) UNIQUE NOT NULL,
          hash varchar(40) NOT NULL, -- sha1 hex encoded hash of the file name and contents, to ensure it hasn't been altered since applying the migration
          executed_at timestamp DEFAULT current_timestamp
        );
    `);

    log(`Migrations table with name '${migrationSchemaName}.${migrationTableName}' has been created!`)
  }
  
  return appliedMigrations
}

/** Work out which migrations to apply */
function filterMigrations(
  migrations: Array<Migration>,
  appliedMigrations: Record<number, Migration | undefined>,
) {
  const notAppliedMigration = (migration: Migration) =>
    !appliedMigrations[migration.id]

  return migrations.filter(notAppliedMigration)
}

/** Logs the result */
function logResult(completedMigrations: Array<Migration>, log: Logger) {
  if (completedMigrations.length === 0) {
    log("No migrations applied")
  } else {
    log(
      `Successfully applied migrations: ${completedMigrations.map(
        ({name}) => name,
      )}`,
    )
  }
}

/** Check whether table exists in postgres - http://stackoverflow.com/a/24089729 */
async function doesTableExist(client: BasicPgClient, tableName: string, schemaName: string) {
  const result = await client.query(SQL`SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = '${schemaName}'
    AND table_name = '${tableName}'
  );`)

  return result.rows.length > 0 && result.rows[0].exists
}
