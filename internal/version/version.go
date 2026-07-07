// Package version identifica o build dos binários do tracker. Commit e BuiltAt são
// injetados via -ldflags no build do Dockerfile (build-args preenchidos pelo
// deploy.sh a partir do git — o .dockerignore exclui o .git, então o buildinfo do Go
// não tem vcs dentro do container). Em builds locais direto no repo (go build/run),
// cai no vcs.revision do buildinfo. Sem nada, "dev".
package version

import "runtime/debug"

var (
	// Commit é o sha curto do git do build (-X .../internal/version.Commit=<sha>).
	Commit string
	// BuiltAt é a data ISO-8601 do build (-X .../internal/version.BuiltAt=<data>).
	BuiltAt string
)

func init() {
	if Commit == "" {
		if bi, ok := debug.ReadBuildInfo(); ok {
			var rev, modified string
			for _, s := range bi.Settings {
				switch s.Key {
				case "vcs.revision":
					rev = s.Value
				case "vcs.modified":
					modified = s.Value
				case "vcs.time":
					if BuiltAt == "" {
						BuiltAt = s.Value
					}
				}
			}
			if len(rev) > 7 {
				rev = rev[:7]
			}
			if rev != "" && modified == "true" {
				rev += "-dirty"
			}
			Commit = rev
		}
	}
	if Commit == "" {
		Commit = "dev"
	}
}
