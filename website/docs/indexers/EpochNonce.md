import sql from '!!raw-loader!@site/static/sql/epochnonce.sql';
import {DbSyncSql} from '@site/src/components/Common.js';

# EpochNonce

Indexes nonces for each epoch. Outputs to sqlite.

This is a very low volume indexer as nonce is a short code and epoch
is five days.

<DbSyncSql sql={sql} />
