import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'wkmigrate',
  tagline: 'Automated workflow migration from Azure Data Factory to Databricks jobs.',

  url: 'https://ghanse.github.io',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/wkmigrate/',
  trailingSlash: true,

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'ghanse', // Usually your GitHub org/user name.
  projectName: 'wkmigrate', // Usually your repo name.

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'throw',
  onDuplicateRoutes: 'throw',
  onBrokenAnchors: 'throw',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  plugins: [
    async (context, options) => {
      return {
        name: "docusaurus-plugin-tailwindcss",
        configurePostCss(postcssOptions) {
          postcssOptions.plugins = [
            require('tailwindcss'),
            require('autoprefixer'),
          ];
          return postcssOptions;
        },
      }
    },
    'docusaurus-plugin-image-zoom',
    'docusaurus-lunr-search'
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          // routeBasePath: '/',
          sidebarPath: './sidebars.ts',
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/ghanse/wkmigrate/tree/main/docs/wkmigrate/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],


  themeConfig: {
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: false,
    },
    navbar: {
      title: 'wkmigrate',
      items: [
        {
          type: 'search',
          position: 'right',
        },
        {
          href: 'https://github.com/ghanse/wkmigrate',
          position: 'right',

          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },

      ],
    },
    footer: {
      links: [
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Databricks. Docs built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.oneLight,
      darkTheme: prismThemes.oneDark,
    },
    zoom: {
      selector: 'article img',
      background: {
        light: '#F8FAFC',
        dark: '#F8FAFC',
      },
    }
  } satisfies Preset.ThemeConfig,
};

export default config;
