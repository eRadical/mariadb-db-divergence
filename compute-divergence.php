#!/usr/bin/env php
<?php
declare(strict_types=1);

/**
 * Present help for more than 2 params
 */
if ($argc > 2) {
    echo <<<EOF
Only one argument is allowed the filename that contains connection details:
$argv[0] compute-divergence.yaml

EOF;
}

const INFORMATION_SCHEMA_TABLE = [
    'ENGINE',
    'VERSION',
    'ROW_FORMAT',
    'TABLE_COLLATION',
    'CREATE_OPTIONS',
    ];
const INFORMATION_SCHEMA_COLUMNS = [
    'ORDINAL_POSITION',
    'COLUMN_DEFAULT',
    'IS_NULLABLE',
    'CHARACTER_SET_NAME',
    'COLLATION_NAME',
    'COLUMN_TYPE',
    'EXTRA',
    'COLUMN_COMMENT'
    ];

global $srcConnection;
global $dstConnection;

computeDivergence();
function computeDivergence(string $configFile = 'compute-divergence.yaml'): void {
    global $srcConnection, $dstConnection;

    $f = yaml_parse_file($configFile);

    $srcConnection = new PDO('mysql:host=' . $f['source']['host'] . ';dbname=' . $f['source']['database'], $f['source']['user'], $f['source']['password'], [
        PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8',
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ]);

    $dstConnection = new PDO('mysql:host=' . $f['destination']['host'] . ';dbname=' . $f['destination']['database'], $f['destination']['user'], $f['destination']['password'], [
        PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8',
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ]);

    compareDatabases();
    compareTables();
}

function compareDatabases(): void {
    global $srcConnection, $dstConnection;

    $sql = 'SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = DATABASE()';

    $srcDb = $srcConnection->query($sql)->fetch();
    $dstDb = $dstConnection->query($sql)->fetch();

    if ($srcDb['DEFAULT_CHARACTER_SET_NAME'] != $dstDb['DEFAULT_CHARACTER_SET_NAME']) {
        printf("DB ::: DIFF: DEFAULT_CHARACTER_SET_NAME SRC: %s DST: %s\n", $srcDb['DEFAULT_CHARACTER_SET_NAME'], $dstDb['DEFAULT_CHARACTER_SET_NAME']);
    }
    if ($srcDb['DEFAULT_COLLATION_NAME'] != $dstDb['DEFAULT_COLLATION_NAME']) {
        printf("DB ::: DIFF: DEFAULT_COLLATION_NAME SRC: %s DST: %s\n", $srcDb['DEFAULT_COLLATION_NAME'], $dstDb['DEFAULT_COLLATION_NAME']);
    }
}

function compareTables() {
    global $srcConnection, $dstConnection;

    $cols = implode(", ", INFORMATION_SCHEMA_TABLE);
    $sql = "SELECT TABLE_NAME, $cols FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()	AND TABLE_TYPE = 'BASE TABLE'";

    $srcTables = reorganizeByTable($srcConnection->query($sql)->fetchAll());
    $dstTables = reorganizeByTable($dstConnection->query($sql)->fetchAll());

    $akSrcTables = array_keys($srcTables);
    $akDstTables = array_keys($dstTables);

    $extraTables = array_diff($akDstTables, $akSrcTables);
    $missingTables = array_diff($akSrcTables, $akDstTables);

    if (count($missingTables) > 0) {
        printf("TABLES_MISSING ::: %s\n", implode(", ", $missingTables));
    }
    if (count($extraTables) > 0) {
        printf("TABLES_EXTRA ::: %s\n", implode(", ", $extraTables));
    }

    $commonTables = array_intersect($akSrcTables, $akDstTables);
    foreach ($commonTables as $table) {
        compareOneTable($table, $srcTables[$table], $dstTables[$table]);
    }
}

function reorganizeByTable(array $resultSet): array {
    $byTable = [];
    foreach ($resultSet as $t) {
        $tableName = $t['TABLE_NAME'];
        unset($t['TABLE_NAME']);
        $byTable[ $tableName ] = $t;
        unset($tableName);
    }
    return $byTable;
}

function compareOneTable(string $table, array $srcInfo, array $dstInfo): void {
    foreach (INFORMATION_SCHEMA_TABLE as $i) {
        if ($srcInfo[$i] != $dstInfo[$i]) {
            printf("TABLE ::: %s ::: DIFF: %s SRC: %s DST: %s  \n", $table, $i, $srcInfo[$i], $dstInfo[$i]);
        }
    }
    compareColumns($table);
}

function compareColumns(string $table) {
    global $srcConnection, $dstConnection;

    $cols = implode(', ', INFORMATION_SCHEMA_COLUMNS);
    $sql = "SELECT COLUMN_NAME, $cols FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";

    $srcStmt = $srcConnection->prepare($sql);
    $srcStmt->execute([$table]);
    $srcCols = reorganizeByCol($srcStmt->fetchAll());

    $dstStmt = $dstConnection->prepare($sql);
    $dstStmt->execute([$table]);
    $dstCols = reorganizeByCol($dstStmt->fetchAll());

    $akSrcCols = array_keys($srcCols);
    $akDstCols = array_keys($dstCols);

    $extraCols = array_diff($akDstCols, $akSrcCols);
    $missingCols = array_diff($akSrcCols, $akDstCols);

    if (count($missingCols) > 0) {
        printf("TABLE ::: %s ::: COLUMNS_MISSING ::: %s\n", $table, implode(", ", $missingCols));
    }
    if (count($extraCols) > 0) {
        printf("TABLE ::: %s ::: COLUMNS_EXTRA ::: %s\n", $table, implode(", ", $extraCols));
    }

    $commonCols = array_intersect($akSrcCols, $akDstCols);
    foreach ($commonCols as $c) {
        compareOneColumn($table, $c, $srcCols[$c], $dstCols[$c]);
    }

}

function reorganizeByCol(array $resultSet): array {
    $byCol = [];
    foreach ($resultSet as $c) {
        $colName = $c['COLUMN_NAME'];
        unset($c['COLUMN_NAME']);
        $byCol[$colName] = $c;
        unset($colName);
    }
    return $byCol;
}

function compareOneColumn($table, $column, $srcInfo, $dstInfo) {
    foreach (INFORMATION_SCHEMA_COLUMNS as $i) {
        if ($srcInfo[$i] != $dstInfo[$i]) {
            printf("TABLE ::: %s ::: COLUMN ::: %s ::: DIFF: %s SRC: %s DST: %s  \n", $table, $column, $i, $srcInfo[$i], $dstInfo[$i]);
        }
    }
}