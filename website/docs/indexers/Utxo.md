import sql from '!!raw-loader!@site/static/sql/utxo.sql';
import {DbSyncSql} from '@site/src/components/Common.js';

# Utxo

Indexes tx outputs: value, address, datum, spending tx.

<DbSyncSql sql={sql} />
