import sql from '!!raw-loader!@site/static/sql/blockbasics.sql';
import {DbSyncSql} from '@site/src/components/Common.js';

# BlockBasics

Indexes slots and block header hashes. Outputs to sqlite.

Useful for setting the starting point for other indexerse

<DbSyncSql sql={sql} />
