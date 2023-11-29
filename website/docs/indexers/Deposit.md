import sql from '!!raw-loader!@site/static/sql/deposit.sql';
import {DbSyncSql} from '@site/src/components/Common.js';

# Deposit

Indexes deposits for specified addresses.

Useful for
- seeing all incoming transactions for an address;
- waiting and reacting for a transaction to arrive;

<DbSyncSql sql={sql} />
