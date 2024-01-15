import sql from '!!raw-loader!@site/static/sql/scripttx.sql';
import {DbSyncSql} from '@site/src/components/Common.js';

# ScriptTx

Indexes all scripts and their hashes in transaction bodies.

<DbSyncSql sql={sql} />
