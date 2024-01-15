import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

import { useState, useEffect } from "react";


const indexersLink = (text) => <a href='./docs/category/indexers'>{text}</a>
const indexerLink = (name, text) => <a href='./docs/indexers'>{text}</a>

const FeatureList = [
  {
    title: 'Custom indexers',
    Svg: require('@site/static/img/undraw_docusaurus_tree.svg').default,
    description: (
      <>
        <p>
          Quickly write a custom indexer by picking the data that you need from the blockchain
          and storing it however you want.
        </p>
        <p>
          Parallelism, profiling and logging are brought to you by the framework.
        </p>
      </>
    )
  },

  {
    title: 'Deploy anywhere',
    Svg: require('@site/static/img/undraw_docusaurus_tree.svg').default,
    description: (
      <>
        Easily build your indexer into a docker container and run anywhere, including Apple silicon Macs.
      </>
    )
  },

  {
    title: 'Data matches Cardano DB Sync',
    Svg: require('@site/static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        Every {indexersLink('indexer in Mafoc')} corresponds to a subset of
        Cardano DB Sync. Testing against it we know we have the right data.
      </>
    ),
  },

  {
    title: 'For users',
    Svg: require('@site/static/img/undraw_docusaurus_tree.svg').default,
    description: (
      <>
        Run any of the predefined indexers out of the box, e.g
        monitor {indexerLink('Deposit', 'incoming transactions')} or {indexerLink('AddressBalance', 'balance')} of
        an address.
      </>
    ),
  },
];

function prefetch(domain) {
  const opts = {
    credentials: 'include',
    method: "HEAD",
    mode: 'no-cors',
  }
  return fetch(`https://${domain}`, opts)
}

async function ws_main(setBlocks, setWsOpen) {
  const ngrokDomain = 'stable-worm-urgently.ngrok-free.app'
  await prefetch(ngrokDomain)
  const ws = new WebSocket(`wss://${ngrokDomain}/blockstream`) // protocols
  ws.addEventListener('open', function (msg) { setWsOpen(() => true); })
  ws.addEventListener('close', function (msg) { setWsOpen(() => false); })
  ws.addEventListener('message', async function (msg) {
    const msgText = await msg.data.text();
    setBlocks((_arr) => {
      return JSON.parse(msgText)
    })
  })
}

function ChainExplorer() {
  const [block, setBlock] = useState(undefined)
  const [wsOpen, setWsOpen] = useState(undefined)
  useEffect(() => { ws_main(setBlock, setWsOpen) }, [])

  const Svg = require('@site/static/img/undraw_docusaurus_tree.svg').default
  const color = wsOpen ? "green" : "gray";
  const boxShadowValue = '0 0 10px '+ color
  return (
    <div className={clsx('col col--4')} style={{backgroundColor: 'transparent'}}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <h3>Chain explorer</h3>
        <p>Block hashes are not a link yet. But the block hash stream is <em>live</em>.</p>
        <p>
          <span
            style={{ width: '10rem'
                     , textOverflow: 'ellipsis'
                     , whiteSpace: 'nowrap'
                     , overflow:'hidden'
                     , display: 'inline-block'
                   }}
          >{displayBlock(block)}</span>
          <span
            style={
              {
                backgroundColor: color
                , boxShadow: boxShadowValue
                , display: 'inline-block'
                , width: '1rem'
                , height: '1rem'
                , borderRadius: '0.5rem'
                , verticalAlign: 'super'
                , marginLeft: '0.6rem'
              }} />
        </p>
      </div>
    </div>
  )
}

function displayBlock(block) {
  if (block === undefined) return 'connecting..';
  else {
    const rf = block['RollForward']
    if (rf) return rf.blockHash
    else {
      const rb = block['RollBackward']
      if (rb) return rb.blockHash
      else {
        console.debug("Didn't receive a proper local chainsync event json..")
        return ''
      }
    }
  }
}

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
          {
            ChainExplorer()
          }
        </div>
      </div>
    </section>
  );
}