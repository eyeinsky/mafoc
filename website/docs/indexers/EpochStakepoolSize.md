import sql from '!!raw-loader!@site/static/sql/epochstakepoolsize.sql';
import {DbSyncSql} from '@site/src/components/Common.js';

# EpochStakepoolSize

Indexes stakepool sizes in absolute ADA for each epoch. Outputs to sqlite.

<DbSyncSql sql={sql} />
