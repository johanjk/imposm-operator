package imposm

import (
	"strings"
	"testing"
)

// FuzzArgs checks that no accepted input lets a value land in a flag slot:
// every argument is either a known flag or the value right after a flag that
// takes one, and no value starts with '-'.
func FuzzArgs(f *testing.F) {
	f.Add("/data/imposm.json", "/data/x.osm.pbf", "import", "public", "backup", "/d/000/004/652.osc.gz")
	f.Add("/c", "-write", "-x", "y", "z", "-force")
	f.Add("/c", "/a b", "a", "a", "a", "/a\nb.osc.gz")
	f.Fuzz(func(t *testing.T, cfg, pbf, imp, prod, back, diffFile string) {
		sch := Schemas{imp, prod, back}
		flagsWithValue := map[string]bool{
			"-config": true, "-read": true, "-dbschema-import": true,
			"-dbschema-production": true, "-dbschema-backup": true,
		}
		boolFlags := map[string]bool{
			"-overwritecache": true, "-diff": true, "-write": true, "-optimize": true,
			"-deployproduction": true, "-revertdeploy": true, "-removebackup": true, "-commit-latest": true,
		}
		check := func(args []string, positional int) {
			if len(args) == 0 || (args[0] != "import" && args[0] != "diff") {
				t.Fatalf("bad subcommand: %q", args)
			}
			rest := args[1 : len(args)-positional]
			for i := 0; i < len(rest); i++ {
				a := rest[i]
				switch {
				case flagsWithValue[a]:
					i++
					if i >= len(rest) || strings.HasPrefix(rest[i], "-") {
						t.Fatalf("flag %s has no safe value in %q", a, args)
					}
				case boolFlags[a]:
				default:
					t.Fatalf("unexpected argument %q in %q", a, args)
				}
			}
			for _, p := range args[len(args)-positional:] {
				if strings.HasPrefix(p, "-") {
					t.Fatalf("positional %q looks like a flag", p)
				}
			}
		}
		for _, step := range []ImportStep{StepRead, StepWrite, StepOptimize, StepDeploy, StepRevert, StepRemoveBack} {
			if args, err := ImportArgs(step, cfg, pbf, sch); err == nil {
				check(args, 0)
			}
		}
		if args, err := DiffArgs(cfg, prod, []string{diffFile}); err == nil {
			check(args, 1)
		}
	})
}
