import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';


const indexersLink = (text) => <a href='./docs/category/indexers'>{text}</a>
const indexerLink = (name, text) => <a href='./docs/indexers'>{text}</a>

const FeatureList = [
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

  {
    title: 'For developers',
    Svg: require('@site/static/img/undraw_docusaurus_tree.svg').default,
    description: (
      <>
        Docusaurus lets you focus on your docs, and we&apos;ll do the chores. Go
        ahead and move your docs into the <code>docs</code> directory.
      </>
    ),
  },

  {
    title: 'Matches Cardano DB Sync',
    Svg: require('@site/static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        Every {indexersLink('indexer in Mafoc')} corresponds to some subset of
        Cardano DB Sync. Having tests for that we know we have the right data.
      </>
    ),
  },

  {
    title: 'Propose an indexer',
    Svg: require('@site/static/img/undraw_docusaurus_tree.svg').default,
    description: (
      <>
        Wish an indexer existed for some type of data but
        doesn't? <a href='https://github.com/eyeinsky/mafoc/issues/new?title=[new indexer]'>Propose it here</a>.
      </>
    ),
  },
];

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
        </div>
      </div>
    </section>
  );
}
