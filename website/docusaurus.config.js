// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Replibyte',
  tagline: 'Seed your dev database with real data',
  url: 'https://www.replibyte.com',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'Qovery', // Usually your GitHub org/user name.
  projectName: 'replibyte', // Usually your repo name.

  presets: [
    [
      '@docusaurus/preset-classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/Qovery/replibyte/webiste/',
          remarkPlugins: [require('mdx-mermaid')],
        },
        blog: {
          showReadingTime: true,
          editUrl:
            'https://github.com/Qovery/replibyte/website/',
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
      navbar: {
        title: 'Replibyte',
        logo: {
          alt: 'Replibyte Logo',
          src: 'img/logo.svg',
        },
        items: [
          {
            type: 'doc',
            docId: 'introduction',
            position: 'left',
            label: 'Documentation',
          },
          {
            href: 'https://discord.qovery.com',
            label: 'Discord',
            position: 'left',
          },
          {
            href: 'https://github.com/Qovery/replibyte',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        //style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Tutorial',
                to: '/docs/introduction',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Discord',
                href: 'https://discord.qovery.com',
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/Qovery_',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/Qovery/replibyte',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} RepliByte from <a href="https://www.qovery.com">Qovery</a>`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
      colorMode: {
        defaultMode: 'dark'
      }
    }),
};

module.exports = config;
