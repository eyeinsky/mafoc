// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

const mafocGithub = 'https://github.com/eyeinsky/mafoc';


/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Mafoc',
  tagline: 'The Cardano Indexer Library',
  favicon: 'img/favicon.ico',

  url: 'https://captainhaskell.ee',
  baseUrl: '/mafoc/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  // organizationName: 'eyeinsky',
  // projectName: 'mafoc',

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: mafocGithub + '/tree/website/website/', // For "edit this page" links.
        },
        blog: {
          showReadingTime: true,
          editUrl: mafocGithub + '/tree/website/website/', // For "edit this page" links.
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      image: 'img/docusaurus-social-card.jpg',

      colorMode: {
        defaultMode: 'dark',
        disableSwitch: false,
        respectPrefersColorScheme: false,
      },

      navbar: {
        title: 'Mafoc',
        logo: {
          alt: 'My Site Logo',
          src: 'img/logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'tutorialSidebar',
            position: 'left',
            label: 'Docs',
          },
          {
            type: 'docSidebar',
            sidebarId: 'tutorialSidebar',
            position: 'left',
            label: 'Haddock',
            href: 'pathname:///haddock',
          },
          // {to: '/blog', label: 'Blog', position: 'left'},
          {
            href: mafocGithub,
            label: 'GitHub',
            position: 'left',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Tutorial',
                to: '/docs/intro',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              // {
              //   label: 'Stack Overflow',
              //   href: 'https://stackoverflow.com/questions/tagged/docusaurus',
              // },
              {
                label: 'Discord',
                href: 'https://discord.gg/bgUE4csP',
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/Mafoc_Cardano',
              },
            ],
          },
          {
            title: 'More',
            items: [
              // {
              //   label: 'Blog',
              //   to: '/blog',
              // },
              {
                label: 'GitHub',
                href: mafocGithub,
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Mafoc, Inc. Built with Docusaurus.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
