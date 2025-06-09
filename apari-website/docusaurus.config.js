// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'APARI Federated Learning Online Document',
  tagline: 'Intelligent Critical Care Center, University of Florida',
  favicon: 'img/ic3-logo.png',

  // Set the production url of your site here
  url: 'https://prisma-presearch.github.io',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/APARI_Federated_Learning/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'Prisma-pResearch', // Usually your GitHub org/user name.
  projectName: 'APARI_Federated_Learning', // Usually your repo name.

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
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/prisma-presearch/APARI_Federated_Learning/edit/main/apari-website/',
        },
        // blog: {
        //   showReadingTime: true,
        //   // Please change this to your repo.
        //   // Remove this to remove the "edit this page" links.
        //   editUrl:
        //     'https://github.com/prisma-presearch/APARI_Federated_Learning/edit/main/apari-website/',
        // },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  plugins:[
    'docusaurus-theme-multi-codetabs',
    // [
    //   require.resolve("@cmfcmf/docusaurus-search-local"),
    //   {
    //     // options
    //     maxSearchResults: 8,
    //   }
    // ],
    [
      '@docusaurus/plugin-ideal-image',
      {
        quality: 85,
        max: 1030,
        min: 640,
        steps: 4,
        size: 360,
        disableInDev: false
      }
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      image: 'img/ic3-logo.png',
      navbar: {
        title: 'APARI',
        logo: {
          alt: 'IC3 Logo',
          src: 'img/ic3-logo.png',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'tutorialSidebar',
            position: 'left',
            label: 'Overview',
          },
          // {to: '/blog', label: 'Blog', position: 'left'},
          {
            type: 'search',
            position: 'right',
          },
          {
            href: 'https://github.com/Prisma-pResearch/APARI_Federated_Learning',
            label: 'GitHub',
            position: 'right',
          },
          {
            href: 'https://github.com/NVIDIA/NVFlare/discussions',
            label: 'FLARE Discussion',
            position: 'right',
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
                label: 'Overview',
                to: '/docs/intro',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Center Website',
                href: 'https://ic3.center.ufl.edu/',
              },
              {
                label: 'Center Twitter',
                href: 'https://twitter.com/UF_IC3',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Center GitHub',
                href: 'https://github.com/Prisma-pResearch?view_as=public',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Intelligent Critical Care Center. Built with Docusaurus.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
      colorMode:{
        disableSwitch: true
      },

      announcementBar: {
        id: 'supportus', // Any value that will identify this message.
        content: "😏 The document is in development... 🤟"
      }
      
    }),

};

module.exports = config;
