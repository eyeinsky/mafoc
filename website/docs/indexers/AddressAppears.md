import sql from '!!raw-loader!@site/static/sql/addressappears.sql';
import {DbSyncSql} from '@site/src/components/Common.js';

# AddressAppears

Indexes the earliest use of every addresses. Outputs to sqlite.

Useful to determine optimal starting points for other indexers that
filter on addresses, e.g utxo, addressbalance, addressdatum, deposit.

<DbSyncSql sql={sql} />
