import { existsSync, readdirSync, readFileSync } from "fs"
import react from "@vitejs/plugin-react"
import { execSync } from "child_process"
import { PATHS } from "../utils.js"
import { defineConfig } from "vite"
import path from "path"

const REPO_ROOT = path.resolve(import.meta.dirname, "..")

// instanceData.js fetches config/ and data/ at runtime relative to BASE_URL.
// The deploy publishes them next to the bundle; in dev (and preview) this
// middleware serves them from the repo root instead.
function serveInstanceData() {
    let base = "/"
    const middleware = (req, res, next) => {
        const url = req.url.split("?")[0]
        const rel = url.startsWith(base) ? url.slice(base.length) : null
        if (!rel || !/^(config|data)\//.test(rel)) return next()
        const file = path.join(REPO_ROOT, rel)
        // Own the 404: falling through would hit the SPA fallback, which
        // serves index.html with 200 — instanceData would parse HTML as TTL.
        if (!existsSync(file)) { res.statusCode = 404; return res.end() }
        res.setHeader("Content-Type", "text/turtle")
        res.end(readFileSync(file))
    }
    return {
        name: "serve-instance-data",
        configResolved(c) { base = c.base },
        configureServer(server) { server.middlewares.use(middleware) },
        configurePreviewServer(server) { server.middlewares.use(middleware) },
    }
}

// Static-file sources have no live harvest. Freeze the commit time of each
// source's static folder (per the PATHS convention) at build time (reflects
// the committed git state)
function staticSourceCommits() {
    const dir = path.join(REPO_ROOT, "sources")
    if (!existsSync(dir)) return {}
    const out = {}
    for (const e of readdirSync(dir, { withFileTypes: true })) {
        const rel = PATHS.staticDir(e.name).replace(/\/$/, "")
        if (!e.isDirectory() || !existsSync(path.join(REPO_ROOT, rel))) continue
        try {
            const iso = execSync(`git log -1 --format=%cI -- "${rel}"`, { cwd: REPO_ROOT, encoding: "utf8" }).trim()
            if (iso) out[rel] = iso
        } catch { /* not committed yet → omit */ }
    }
    return out
}

export default defineConfig({
    base: "/directory-builder/",
    plugins: [react(), serveInstanceData()],
    build: { target: "es2022" },  // top-level await in instanceData.js
    define: {
        __STATIC_SOURCE_COMMITS__: JSON.stringify(staticSourceCommits()),
    },
})
