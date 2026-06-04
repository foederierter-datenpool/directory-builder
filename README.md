# sosuse-directory-builder
Builds a federated directory of social support services from multiple input sources.

## Prerequisites
- Node.js
- Java (for [SPARQL Anything](https://github.com/SPARQL-Anything/sparql.anything), auto-downloaded on first run)

## Setup
```sh
npm install
```

## Run the pipeline
Two ways — both run the same engines, rooted at the instance directory (config/, sources/, data/).

Via command (root = where you invoke):
```sh
npm run pipeline   # ingest + federate
npm run ingest     # fetch + lift only
npm run federate   # clean → map → match → merge → resolve only
```

Or programmatically:
```js
import { Pipeline } from "@directory-builder/core"

const pipeline = new Pipeline() // root defaults to process.cwd()
await pipeline.run() // ingest + federate
```
Outputs &rarr; `data/`

## Run the webapp
The webapp ships with `@directory-builder/core`; this repo holds no webapp code.
```sh
npm run webapp         # dev server against this repo's config/ + data/
npm run webapp:build   # production build → dist/
```

## Deployment
Pushes to `main` trigger `.github/workflows/deploy.yml`, which runs the pipeline, builds the webapp, and force-pushes the result as a single-commit onto the `gh-pages` branch where the static webapp is being served from via GitHub Pages.
