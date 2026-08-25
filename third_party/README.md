# Third-party distribution bundle

`licenses/` is the committed license, notice, and corresponding-source bundle
for the compiled Linux SirenaIX gateway dependency graph. It is generated with
`github.com/google/go-licenses` version `v1.6.0` for both release architectures.
The generator requires the `linux/amd64` and `linux/arm64` results to be byte
identical before accepting them.

Some dependencies, including modules under copyleft licenses such as MPL-2.0,
require more than a license-name inventory. For that reason this directory
preserves the files emitted by `go-licenses save`, including notices, license
texts, and corresponding source where the classifier determines it is needed.
`licenses/MODULES.txt` records the exact compiled module/version graph, and
`licenses/MANIFEST.sha256` provides a deterministic byte-level inventory of the
distributed files.

Regenerate and review the bundle whenever the production gateway dependency
graph changes:

```sh
go install github.com/google/go-licenses@v1.6.0
./scripts/generate-third-party-licenses third_party/licenses.new
diff -ru third_party/licenses third_party/licenses.new
```

The repository's own AGPL license and attribution are distributed separately as
`LICENSE` and `NOTICE.md`. Automated classification and this bundle are useful
compliance evidence, not legal advice or a guarantee that every obligation has
been identified.
