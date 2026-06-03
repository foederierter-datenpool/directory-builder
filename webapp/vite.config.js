import { existsSync, readFileSync } from "fs"
import react from "@vitejs/plugin-react"
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

export default defineConfig({
    base: "/directory-builder/",
    plugins: [react(), serveInstanceData()],
    build: { target: "es2022" },  // top-level await in instanceData.js
})
