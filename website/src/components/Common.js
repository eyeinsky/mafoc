import React from 'react';
import CodeBlock from '@theme/CodeBlock';

export function DbSyncSql({sql, children}) {
  return <>
           <h2>Cardano DB Sync equivalent SQL</h2>
           { sql ? <CodeBlock className='language-sql'>{sql}</CodeBlock> : undefined}
           { children }
         </>
}
