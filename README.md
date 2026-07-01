# BufSim — SQL Query Engine with Buffer Pool Simulation

A from-scratch database engine in C++17 that simulates a page-level buffer pool
with three replacement policies (LRU, MRU, CLOCK) and a full SQL front-end.

## Build

```bash
make          # produces ./bufsim
make clean    # removes binary, *.bin, output.txt
```

Requires: g++ with C++17 support.

## Run

```bash
./bufsim
```

On startup you choose a buffer replacement policy and frame count.
All queries and results are echoed to `output.txt` (append mode across sessions).

## Supported SQL

```sql
-- DDL
CREATE TABLE t (col1 INT, col2 CHAR(20), ...)
DROP TABLE t
ALTER TABLE t ADD [COLUMN] col TYPE

-- DML
INSERT INTO t [(col,...)] VALUES (v1, ...)
UPDATE t SET col = val [, ...] [WHERE ...]
DELETE FROM t [WHERE ...]

-- Queries
SELECT [DISTINCT] * | col [AS alias], COUNT(*), SUM/AVG/MIN/MAX(col) [AS alias]
  FROM t
  [WHERE col op val [AND|OR col op val ...]]
  [GROUP BY col [, ...]]
  [HAVING agg(col) op val]
  [ORDER BY col [ASC|DESC] [, ...]]
  [LIMIT n [OFFSET m]]

SELECT ... FROM t1 JOIN t2 ON t1.col = t2.col
  [WHERE ...]  [ORDER BY ...]  [LIMIT n [OFFSET m]]

-- Utility
SHOW TABLES
EXIT / QUIT
```

### Operators
`=`  `!=`  `<>`  `>`  `<`  `>=`  `<=`  `LIKE`  `IS NULL`  `IS NOT NULL`

## Architecture

| File | Role |
|---|---|
| `bufferManager.hpp/cpp` | LRU, MRU, CLOCK buffer pool; Frame, BufStats |
| `catalog.hpp/cpp` | Schema registry; persists to `catalog.json` |
| `sqlParser.hpp/cpp` | Tokenizer + recursive parser → ParsedQuery |
| `queryExecutor.hpp/cpp` | All SQL executors; buffer-coherent read/write |
| `main.cpp` | REPL shell |

## Buffer Stats

After every SELECT or JOIN the engine prints per-query stats:

```
--- Buffer Stats [SELECT employees] ---
Page Accesses : 4
Disk Reads    : 1
Page Hits     : 3
Page Misses   : 1
Hit Rate      : 75.00%
```

## Storage Format

Each table is stored as a binary file (`tablename.bin`) of 4096-byte pages.
Each page begins with a 4-byte `int` record count followed by fixed-width records.
INT columns occupy 4 bytes; CHAR(N) columns occupy N bytes (null-padded).
