# sosuse-directory-builder
Builds a federated directory of social support services from multiple input sources.

## How it works
This repo is a **use case** of [`@directory-builder/core`](https://github.com/foederierter-datenpool/directory-builder-core)
and holds no engine or webapp code — only what is specific to this federation:

- **Decisions** live in `config/federation.ttl`: the sources and their facts
  (URL, format, lift params), the target schemas and field mappings, the
  match/merge/resolve rules, run parameters, exporters, repository URL and
  title. `config/match-knowledge.ttl` adds curated `owl:sameAs` pairs.
- **Per-source code** lives in `sources/<name>/`: a `fetch.js` (how to get the
  data), an `extract.sparql` (how to extract entities from its lifted RDF),
  optional `transform-*.sparql`, and committed `static/` files for sources
  without an API.
- **Webapp material** lives in `webapp/`: the About page prose, the Query
  page's starting query, and the exporters the Download page loads at runtime.

Everything else is convention: every file path follows from the source names,
so the config contains no paths at all. The engines journal each executed step
as p-plan RDF (`data/ingest/ingest-log.ttl`, `data/pipeline/federate-log.ttl`),
and the webapp renders those journals and the pipeline's artifacts directly —
the site is a pure function of `config/` + `data/`, fetched at runtime.

The pipeline: fetch → lift (to RDF) → extract → map (onto the target schemas) →
match (cluster duplicates across sources) → merge → resolve (one value per
field), each stage written to `data/`.

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
npm run federate   # extract → map → match → merge → resolve only
```

Or programmatically:
```js
import { Pipeline } from "@directory-builder/core"

const pipeline = new Pipeline() // root defaults to process.cwd()
await pipeline.run() // ingest + federate
```
Outputs &rarr; `data/`

## Run the webapp
The webapp ships with `@directory-builder/core`; this repo holds no webapp
code — only the modules and prose under `webapp/` it injects at runtime.
```sh
npm run webapp         # dev server against this repo's config/ + data/
npm run webapp:build   # production build → webapp/dist/
```

## Deployment
Pushes to `main` trigger `.github/workflows/deploy.yml`, which runs the pipeline, builds the webapp, and force-pushes the result as a single-commit onto the `gh-pages` branch where the static webapp is being served from via GitHub Pages.
