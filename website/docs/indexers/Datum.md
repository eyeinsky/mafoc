import sql from '!!raw-loader!@site/static/sql/datum.sql';
import {DbSyncSql} from '@site/src/components/Common.js';

# Datum

Indexes datums and their hashes. Outputs to sqlite.

<DbSyncSql sql={sql} />
