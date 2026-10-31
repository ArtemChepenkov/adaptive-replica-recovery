# Browser dependencies

Pinned distributions served from this directory; no CDN is needed at runtime.

- Chart.js 4.5.1: `chart.umd.js`, MIT, `chartjs-LICENSE.md`.
- js-yaml 4.1.1: `js-yaml.min.js`, MIT, `js-yaml-LICENSE`.
- Chart.js includes @kurkle/color 0.3.2, MIT, `kurkle-color-LICENSE.md`.

`sources.json` records exact download URLs, byte lengths and SHA-256 hashes.
The UMD Chart.js build is used with ordinary script tags as described in the
[official integration guide](https://www.chartjs.org/docs/latest/getting-started/integration.html).
Package versions and licenses were checked against upstream tagged metadata:
[Chart.js](https://github.com/chartjs/Chart.js/blob/v4.5.1/package.json),
[js-yaml](https://github.com/nodeca/js-yaml/blob/4.1.1/package.json).
