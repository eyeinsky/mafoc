import sql from '!!raw-loader!@site/static/sql/addressbalance.sql';
import {DbSyncSql} from '@site/src/components/Common.js';

# AddressBalance

Index balance changes for addresses. Outputs JSON to stdout.

<DbSyncSql sql={sql} />
