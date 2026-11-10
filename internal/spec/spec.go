// Package spec defaults, validates and decodes ImposmSyncSpec. The same code is
// used by the CRD controller, the ConfigMap controller and `imposm-operator
// run`, so a spec means the same thing in all three modes. Validation mirrors
// the CRD's OpenAPI markers because ConfigMap and CronJob specs never pass
// through the apiserver's schema validation.
package spec

import (
	"fmt"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/yaml"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/limitto"
)

// MaxSpecSize bounds decoded specs.
const MaxSpecSize = 256 << 10

// Decode strictly decodes YAML or JSON (unknown fields are errors), applies
// defaults and validates.
func Decode(b []byte) (*v1.ImposmSyncSpec, error) {
	if len(b) > MaxSpecSize {
		return nil, fmt.Errorf("spec larger than %d bytes", MaxSpecSize)
	}
	var s v1.ImposmSyncSpec
	if err := yaml.UnmarshalStrict(b, &s); err != nil {
		return nil, fmt.Errorf("spec: %w", err)
	}
	Default(&s)
	if errs := Validate(&s, field.NewPath("spec")); len(errs) > 0 {
		return nil, errs.ToAggregate()
	}
	return &s, nil
}

// Default fills unset fields, matching the +kubebuilder:default markers.
func Default(s *v1.ImposmSyncSpec) {
	def := func(p *string, v string) {
		if *p == "" {
			*p = v
		}
	}
	def(&s.Schedule, "17 * * * *")
	if s.MaxDiffsPerRun == 0 {
		s.MaxDiffsPerRun = 100
	}
	if s.DiffChunkSize == 0 {
		s.DiffChunkSize = 10
	}
	def(&s.DeletionPolicy, "Retain")
	def(&s.Source.UpdateFrequency, "day")
	def(&s.Source.Checksum, "md5")
	if s.Database.Port == 0 {
		s.Database.Port = 5432
	}
	def(&s.Database.SSLMode, "require")
	def(&s.Database.Schemas.Import, "import")
	def(&s.Database.Schemas.Production, "public")
	def(&s.Database.Schemas.Backup, "backup")
	def(&s.Database.Schemas.State, "imposm_operator")
	if s.Imposm.SRID == 0 {
		s.Imposm.SRID = 3857
	}
	if et := s.Imposm.ExpireTiles; et != nil {
		if et.Zoom == 0 {
			et.Zoom = 14
		}
		if r := et.Redis; r != nil {
			def(&r.Stream, "expiretiles")
			def(&r.Field, "tiles")
			if r.MaxLen == nil {
				n := int64(100000)
				r.MaxLen = &n
			}
		}
	}
	if s.Imposm.CacheCheckpointLimit == nil {
		q := resource.MustParse("4Gi")
		s.Imposm.CacheCheckpointLimit = &q
	}
	if s.Storage.Size == nil && s.Storage.ExistingClaim == "" && s.Storage.Volume == nil {
		q := resource.MustParse("20Gi")
		s.Storage.Size = &q
	}
	if s.Deploy.Auto == nil {
		t := true
		s.Deploy.Auto = &t
	}
	if s.Deploy.SyncDuringReimport == "" {
		s.Deploy.SyncDuringReimport = v1.SyncDuringReimportAuto
	}
	if s.Runner.ActiveDeadline == nil {
		s.Runner.ActiveDeadline = &metav1.Duration{Duration: 24 * time.Hour}
	}
}

var (
	identRe   = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
	prefixRe  = regexp.MustCompile(`^[a-z][a-z0-9]{0,19}_?$`)
	cronParse = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
)

// Limits on mapping references (also in the CRD schema): the controller
// loads every referenced part into memory.
const (
	MaxMappingSources = 32
	MaxMappingKeys    = 64
)

// Validate checks a defaulted spec.
func Validate(s *v1.ImposmSyncSpec, p *field.Path) field.ErrorList {
	var errs field.ErrorList
	add := func(e *field.Error) { errs = append(errs, e) }

	// source
	sp := p.Child("source")
	// The spec is not secret (status, logs, the state database show the
	// URLs): credentials in them are refused.
	checkURL := func(name, raw string) {
		u, err := url.Parse(raw)
		switch {
		case err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "":
			add(field.Invalid(sp.Child(name), raw, "must be an absolute http(s) URL"))
		case u.User != nil:
			add(field.Invalid(sp.Child(name), u.Redacted(), "must not contain credentials"))
		}
	}
	checkURL("baseURL", s.Source.BaseURL)
	if r := s.Source.ReplicationBaseURL; r != "" {
		checkURL("replicationBaseURL", r)
	}
	for name, v := range map[string]string{"pbfPath": s.Source.PBFPath, "replicationPath": s.Source.ReplicationPath} {
		if err := relPath(v); err != "" {
			add(field.Invalid(sp.Child(name), v, err))
		}
	}
	if !slices.Contains([]string{"minute", "hour", "day"}, s.Source.UpdateFrequency) {
		add(field.NotSupported(sp.Child("updateFrequency"), s.Source.UpdateFrequency, []string{"minute", "hour", "day"}))
	}
	if !slices.Contains([]string{"md5", "none"}, s.Source.Checksum) {
		add(field.NotSupported(sp.Child("checksum"), s.Source.Checksum, []string{"md5", "none"}))
	}

	// database
	dp := p.Child("database")
	if s.Database.Host == "" || strings.ContainsAny(s.Database.Host, " /?#@") && !strings.HasPrefix(s.Database.Host, "/") {
		add(field.Invalid(dp.Child("host"), s.Database.Host, "must be a host name, IP or socket directory"))
	}
	if s.Database.Port < 1 || s.Database.Port > 65535 {
		add(field.Invalid(dp.Child("port"), s.Database.Port, "must be 1-65535"))
	}
	// No spaces, quotes, backslashes or "=": imposm reads options (such as
	// prefix=) from the same connection string.
	for name, v := range map[string]string{"database": s.Database.Database, "user": s.Database.User} {
		if v == "" || len(v) > 63 || strings.ContainsAny(v, "\x00\n\r\t '\"\\=") {
			add(field.Invalid(dp.Child(name), v, "must be 1-63 characters without spaces, quotes, backslashes or '='"))
		}
	}
	if !slices.Contains([]string{"disable", "require", "verify-ca", "verify-full"}, s.Database.SSLMode) {
		add(field.NotSupported(dp.Child("sslMode"), s.Database.SSLMode, []string{"disable", "require", "verify-ca", "verify-full"}))
	}
	if s.Database.PasswordSecretRef != nil && s.Database.PgpassSecretRef != nil {
		add(field.Forbidden(dp.Child("pgpassSecretRef"), "set at most one of passwordSecretRef and pgpassSecretRef"))
	}
	for name, ref := range map[string]*corev1.SecretKeySelector{
		"passwordSecretRef": s.Database.PasswordSecretRef,
		"pgpassSecretRef":   s.Database.PgpassSecretRef,
	} {
		if ref == nil {
			continue
		}
		if !validName(ref.Name) {
			add(field.Invalid(dp.Child(name, "name"), ref.Name, "must be a valid Secret name"))
		}
		if !validKey(ref.Key) {
			add(field.Invalid(dp.Child(name, "key"), ref.Key, "must be a valid Secret key"))
		}
	}
	schemas := map[string]string{
		"import": s.Database.Schemas.Import, "production": s.Database.Schemas.Production,
		"backup": s.Database.Schemas.Backup, "state": s.Database.Schemas.State,
	}
	seen := map[string]string{}
	for _, name := range []string{"import", "production", "backup", "state"} {
		v := schemas[name]
		if !identRe.MatchString(v) || strings.HasPrefix(v, "pg_") {
			add(field.Invalid(dp.Child("schemas", name), v, "must be a lower-case SQL identifier (not pg_*)"))
		}
		if other, dup := seen[v]; dup {
			add(field.Duplicate(dp.Child("schemas", name), fmt.Sprintf("%s (same as %s)", v, other)))
		}
		seen[v] = name
	}

	// mapping
	mp := p.Child("mapping", "sources")
	if len(s.Mapping.Sources) == 0 {
		add(field.Required(mp, "at least one mapping source"))
	}
	if len(s.Mapping.Sources) > MaxMappingSources {
		add(field.TooMany(mp, len(s.Mapping.Sources), MaxMappingSources))
	}
	seenCM := map[string]bool{}
	for i, src := range s.Mapping.Sources {
		ip := mp.Index(i)
		switch {
		case (src.ConfigMapRef != nil) == (src.Inline != ""):
			add(field.Invalid(ip, "", "exactly one of configMapRef or inline"))
		case src.ConfigMapRef != nil:
			if !validName(src.ConfigMapRef.Name) {
				add(field.Invalid(ip.Child("configMapRef", "name"), src.ConfigMapRef.Name, "must be a valid ConfigMap name"))
			}
			// each ConfigMap and key once: repeats would only multiply the
			// data the controller loads
			if seenCM[src.ConfigMapRef.Name] {
				add(field.Duplicate(ip.Child("configMapRef", "name"), src.ConfigMapRef.Name))
			}
			seenCM[src.ConfigMapRef.Name] = true
			if len(src.ConfigMapRef.Keys) > MaxMappingKeys {
				add(field.TooMany(ip.Child("configMapRef", "keys"), len(src.ConfigMapRef.Keys), MaxMappingKeys))
			}
			seenKey := map[string]bool{}
			for j, k := range src.ConfigMapRef.Keys {
				if !validKey(k) {
					add(field.Invalid(ip.Child("configMapRef", "keys").Index(j), k, "must be a valid ConfigMap key"))
				}
				if seenKey[k] {
					add(field.Duplicate(ip.Child("configMapRef", "keys").Index(j), k))
				}
				seenKey[k] = true
			}
		}
	}

	// imposm
	if s.Imposm.SRID != 3857 && s.Imposm.SRID != 4326 {
		add(field.NotSupported(p.Child("imposm", "srid"), s.Imposm.SRID, []string{"3857", "4326"}))
	}
	if et := s.Imposm.ExpireTiles; et != nil {
		ep := p.Child("imposm", "expireTiles")
		if et.Zoom < 6 || et.Zoom > 18 {
			add(field.Invalid(ep.Child("zoom"), et.Zoom, "must be 6-18 (imposm replaces other values with 14)"))
		}
		if f := et.Files; f != nil {
			validateVolume(&f.Volume, ep.Child("files", "volume"), add)
			if f.SubPath != "" && (strings.HasPrefix(f.SubPath, "/") || relPath(f.SubPath) != "") {
				add(field.Invalid(ep.Child("files", "subPath"), f.SubPath, "must be a relative path"))
			}
		}
		if r := et.Redis; r != nil {
			rp := ep.Child("redis")
			if (r.URL == "") == (r.URLSecretRef == nil) {
				add(field.Invalid(rp, "", "exactly one of url or urlSecretRef"))
			}
			if r.URL != "" {
				if u, err := url.Parse(r.URL); err != nil || (u.Scheme != "redis" && u.Scheme != "rediss") || u.Host == "" {
					add(field.Invalid(rp.Child("url"), r.URL, "must be a redis:// or rediss:// URL"))
				} else if _, hasPw := u.User.Password(); hasPw {
					add(field.Invalid(rp.Child("url"), "<redacted>", "must not contain a password; use urlSecretRef or passwordSecretRef"))
				}
			}
			for name, ref := range map[string]*corev1.SecretKeySelector{"urlSecretRef": r.URLSecretRef, "passwordSecretRef": r.PasswordSecretRef} {
				if ref != nil && (!validName(ref.Name) || !validKey(ref.Key)) {
					add(field.Invalid(rp.Child(name), ref.Name+"/"+ref.Key, "must name a Secret and key"))
				}
			}
			if r.Stream == "" || len(r.Stream) > 256 || r.Field == "" || len(r.Field) > 256 {
				add(field.Invalid(rp, r.Stream+"/"+r.Field, "stream and field must be 1-256 characters"))
			}
			if r.MaxLen != nil && *r.MaxLen < 0 {
				add(field.Invalid(rp.Child("maxLen"), *r.MaxLen, "must not be negative"))
			}
		}
	}

	if q := s.Imposm.CacheCheckpointLimit; q != nil && q.Sign() < 0 {
		add(field.Invalid(p.Child("imposm", "cacheCheckpointLimit"), q.String(), "must not be negative"))
	}

	if tp := s.Imposm.TablePrefix; tp != "" && !prefixRe.MatchString(tp) {
		add(field.Invalid(p.Child("imposm", "tablePrefix"), tp, "must be a lower-case letter followed by up to 19 letters or digits, optionally ending in _"))
	}
	if lt := s.Imposm.LimitTo; lt != nil {
		lp := p.Child("imposm", "limitTo")
		switch {
		case (lt.Inline == "") == (lt.ConfigMapRef == nil):
			add(field.Invalid(lp, "", "exactly one of inline or configMapRef"))
		case lt.Inline != "":
			if _, err := limitto.Validate([]byte(lt.Inline)); err != nil {
				add(field.Invalid(lp.Child("inline"), "<geojson>", err.Error()))
			}
		case !validName(lt.ConfigMapRef.Name) || !validKey(lt.ConfigMapRef.Key):
			add(field.Invalid(lp.Child("configMapRef"), lt.ConfigMapRef.Name+"/"+lt.ConfigMapRef.Key, "must name a ConfigMap and key"))
		}
		if _, err := limitto.ParseBuffer(lt.CacheBufferDegrees); err != nil {
			add(field.Invalid(lp.Child("cacheBufferDegrees"), lt.CacheBufferDegrees, err.Error()))
		}
	}

	// scheduling
	if _, err := cronParse.Parse(s.Schedule); err != nil {
		add(field.Invalid(p.Child("schedule"), s.Schedule, err.Error()))
	}
	if s.MaxDiffsPerRun < 1 || s.MaxDiffsPerRun > 100000 {
		add(field.Invalid(p.Child("maxDiffsPerRun"), s.MaxDiffsPerRun, "must be 1-100000"))
	}
	if s.DiffChunkSize < 1 || s.DiffChunkSize > 1000 {
		add(field.Invalid(p.Child("diffChunkSize"), s.DiffChunkSize, "must be 1-1000"))
	}
	if !slices.Contains([]string{"auto", "always", "never"}, string(s.Deploy.SyncDuringReimport)) {
		add(field.NotSupported(p.Child("deploy", "syncDuringReimport"), s.Deploy.SyncDuringReimport, []string{"auto", "always", "never"}))
	}
	if !slices.Contains([]string{"Retain", "Delete"}, s.DeletionPolicy) {
		add(field.NotSupported(p.Child("deletionPolicy"), s.DeletionPolicy, []string{"Retain", "Delete"}))
	}
	if s.Storage.Size != nil && s.Storage.Size.Sign() <= 0 {
		add(field.Invalid(p.Child("storage", "size"), s.Storage.Size.String(), "must be positive"))
	}
	if v := s.Storage.Volume; v != nil {
		sp := p.Child("storage", "volume")
		if s.Storage.ExistingClaim != "" {
			add(field.Forbidden(sp, "set at most one of existingClaim and volume"))
		}
		validateVolume(v, sp, add)
	}
	if c := s.Storage.ExistingClaim; c != "" && !validName(c) {
		add(field.Invalid(p.Child("storage", "existingClaim"), c, "must be a valid PVC name"))
	}
	if d := s.Runner.ActiveDeadline; d != nil && d.Duration < time.Minute {
		add(field.Invalid(p.Child("runner", "activeDeadline"), d.Duration.String(), "must be at least 1m"))
	}
	rp := p.Child("runner")
	errs = append(errs, metav1validation.ValidateLabels(s.Runner.Labels, rp.Child("labels"))...)
	errs = append(errs, apivalidation.ValidateAnnotations(s.Runner.Annotations, rp.Child("annotations"))...)
	for k := range s.Runner.Labels {
		if reservedKey(k, v1.Group+"/", "app.kubernetes.io/") {
			add(field.Invalid(rp.Child("labels").Key(k), k, "reserved for the operator"))
		}
	}
	for k := range s.Runner.Annotations {
		if reservedKey(k, v1.Group+"/") {
			add(field.Invalid(rp.Child("annotations").Key(k), k, "reserved for the operator"))
		}
	}
	// The database and Redis ports reach internal addresses, the upstream
	// ports public ones only: one port in both would open internal HTTP
	// services (and cloud metadata) to the upstream rule's traffic.
	if len(errs) == 0 {
		e := Egress(s)
		if e.Database > 0 && slices.Contains(e.Upstream, e.Database) {
			add(field.Invalid(p.Child("database", "port"), e.Database, "must differ from the upstream ports (80, 443 and the ports in the source URLs)"))
		}
		if e.Redis > 0 && slices.Contains(e.Upstream, e.Redis) {
			add(field.Invalid(p.Child("imposm", "expireTiles", "redis", "url"), e.Redis, "the Redis port must differ from the upstream ports (80, 443 and the ports in the source URLs)"))
		}
	}
	return errs
}

// reservedKey reports whether a label/annotation key is under one of the
// operator's prefixes (or a subdomain of them, e.g. x.imposm.johanjk.github.io/).
func reservedKey(k string, prefixes ...string) bool {
	for _, pre := range prefixes {
		dom := strings.TrimSuffix(pre, "/")
		if strings.HasPrefix(k, pre) || strings.Contains(k, "."+dom+"/") {
			return true
		}
	}
	return false
}

// validateVolume checks a sync's volume source: exactly one of
// persistentVolumeClaim, csi or nfs (the CRD checks this too; the ConfigMap
// and CronJob front-ends rely on this).
func validateVolume(v *v1.VolumeSource, p *field.Path, add func(*field.Error)) {
	n := 0
	if c := v.PersistentVolumeClaim; c != nil {
		n++
		if !validName(c.ClaimName) {
			add(field.Invalid(p.Child("persistentVolumeClaim", "claimName"), c.ClaimName, "must be a valid PVC name"))
		}
	}
	if v.CSI != nil {
		n++
		if v.CSI.Driver == "" {
			add(field.Required(p.Child("csi", "driver"), ""))
		}
	}
	if v.NFS != nil {
		n++
		if v.NFS.Server == "" || !strings.HasPrefix(v.NFS.Path, "/") {
			add(field.Invalid(p.Child("nfs"), v.NFS.Server+":"+v.NFS.Path, "needs a server and an absolute path"))
		}
	}
	if n != 1 {
		add(field.Invalid(p, "", "set exactly one of persistentVolumeClaim, csi or nfs"))
	}
}

func relPath(v string) string {
	switch {
	case v == "":
		return "required"
	case strings.Contains(v, "://") || strings.HasPrefix(v, "/"):
		return "must be relative to baseURL"
	case strings.ContainsAny(v, "?#\\\x00\n\r"):
		return "must not contain ?, #, backslashes or control characters"
	}
	for _, seg := range strings.Split(v, "/") {
		if seg == ".." {
			return "must not contain .."
		}
	}
	u, err := url.Parse(v)
	if err != nil || u.Scheme != "" || u.Host != "" {
		return "must be a valid relative URL path"
	}
	return ""
}

// validName: a valid ConfigMap, Secret or PVC name.
func validName(n string) bool { return len(k8svalidation.IsDNS1123Subdomain(n)) == 0 }

// validKey: a valid ConfigMap or Secret key.
func validKey(k string) bool { return len(k8svalidation.IsConfigMapKey(k)) == 0 }

// TablePrefix is the prefix imposm puts before every table name: "osm_"
// unless imposm.tablePrefix is set (imposm always separates it with "_").
func TablePrefix(s *v1.ImposmSyncSpec) string {
	p := s.Imposm.TablePrefix
	if p == "" {
		return "osm_"
	}
	if p[len(p)-1] != '_' {
		p += "_"
	}
	return p
}
