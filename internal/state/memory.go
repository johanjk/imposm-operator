package state

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// Memory is an in-process Store with the same semantics as Postgres, for
// engine unit tests. FailNext makes the next call return an error.
type Memory struct {
	mu       sync.Mutex
	targets  map[string]int64
	imports  []*Import
	batches  []*Batch
	locks    map[string]bool
	claims   map[string][]string
	nextID   int64
	FailNext error

	// Tables, when set, are the existing tables (schema.table).
	Tables map[string]bool
	// Dependents: tables that something depends on (DropTables keeps them).
	Dependents map[string]string
	Dropped    []string
	Moved      []TableMove
	// OnMove is called for every schema pair MoveTables moves tables between.
	OnMove func(from, to string)
}

// NewMemory returns an empty store.
func NewMemory() *Memory {
	return &Memory{targets: map[string]int64{}, locks: map[string]bool{}, claims: map[string][]string{}}
}

func (m *Memory) fail() error {
	if err := m.FailNext; err != nil {
		m.FailNext = nil
		return err
	}
	return nil
}

func (m *Memory) id() int64 { m.nextID++; return m.nextID }

func (m *Memory) Migrate(context.Context) error { return nil }
func (m *Memory) Close()                        {}

func (m *Memory) EnsurePostGIS(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fail()
}

type memLock struct {
	m    *Memory
	name string
	lost bool
}

func (l *memLock) Check(context.Context) error {
	if l.lost {
		return errors.New("lock lost")
	}
	return nil
}

func (l *memLock) Release() {
	l.m.mu.Lock()
	defer l.m.mu.Unlock()
	delete(l.m.locks, l.name)
}

func (m *Memory) TryLock(_ context.Context, name string) (Lock, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return nil, err
	}
	if m.locks[name] {
		return nil, nil
	}
	m.locks[name] = true
	return &memLock{m: m, name: name}, nil
}

func (m *Memory) EnsureTarget(_ context.Context, name string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return 0, err
	}
	if id, ok := m.targets[name]; ok {
		return id, nil
	}
	id := m.id()
	m.targets[name] = id
	return id, nil
}

func (m *Memory) CurrentImport(_ context.Context, targetID int64) (*Import, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return nil, err
	}
	for i := len(m.imports) - 1; i >= 0; i-- {
		if imp := m.imports[i]; imp.TargetID == targetID && imp.Phase != PhaseSuperseded {
			c := *imp
			return &c, nil
		}
	}
	return nil, nil
}

func (m *Memory) DeployedImport(_ context.Context, targetID int64) (*Import, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return nil, err
	}
	for _, imp := range m.imports {
		if imp.TargetID == targetID && imp.Phase == PhaseDeployed {
			c := *imp
			return &c, nil
		}
	}
	return nil, nil
}

func (m *Memory) CreateImport(_ context.Context, imp *Import) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return err
	}
	for _, o := range m.imports {
		if o.TargetID == imp.TargetID && o.Phase != PhaseDeployed && o.Phase != PhaseReverted {
			o.Phase = PhaseSuperseded
		}
	}
	if imp.Phase == "" {
		imp.Phase = PhasePending
	}
	imp.ID = m.id()
	imp.CreatedAt, imp.UpdatedAt = time.Now(), time.Now()
	c := *imp
	m.imports = append(m.imports, &c)
	return nil
}

func (m *Memory) UpdateImport(_ context.Context, imp *Import) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return err
	}
	for _, o := range m.imports {
		if imp.Phase == PhaseDeployed && o.TargetID == imp.TargetID && o.ID != imp.ID &&
			(o.Phase == PhaseDeployed || o.Phase == PhaseReverted) {
			o.Phase = PhaseSuperseded
		}
	}
	for _, o := range m.imports {
		if o.ID == imp.ID {
			if imp.Phase == PhaseDeployed && o.DeployedAt == nil {
				now := time.Now()
				imp.DeployedAt = &now
			}
			imp.UpdatedAt = time.Now()
			*o = *imp
			return nil
		}
	}
	return errors.New("state: no such import")
}

func (m *Memory) Applied(_ context.Context, imp *Import) (Position, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return Position{}, err
	}
	pos := Position{Sequence: imp.Sequence, Timestamp: imp.Timestamp}
	for _, b := range m.batches {
		if b.ImportID == imp.ID && b.Status == BatchApplied && b.To > pos.Sequence {
			pos = Position{Sequence: b.To, Timestamp: b.ToTimestamp}
		}
	}
	return pos, nil
}

func (m *Memory) overlaps(b *Batch) bool {
	for _, o := range m.batches {
		if o.ID != b.ID && o.ImportID == b.ImportID && o.Status == BatchApplied && o.From <= b.To && o.To >= b.From {
			return true
		}
	}
	return false
}

func (m *Memory) BeginBatch(_ context.Context, b *Batch) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return err
	}
	if b.Status == "" {
		b.Status = BatchRunning
	}
	if b.Status == BatchApplied && m.overlaps(b) {
		return ErrConflict
	}
	b.ID = m.id()
	b.StartedAt = time.Now()
	c := *b
	m.batches = append(m.batches, &c)
	return nil
}

func (m *Memory) FinishBatch(_ context.Context, b *Batch) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return err
	}
	if b.Status == BatchApplied && m.overlaps(b) {
		return ErrConflict
	}
	for _, o := range m.batches {
		if o.ID == b.ID {
			now := time.Now()
			b.FinishedAt = &now
			*o = *b
			return nil
		}
	}
	return errors.New("state: no such batch")
}

func (m *Memory) Batches(_ context.Context, importID int64) ([]Batch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Batch
	for _, b := range m.batches {
		if b.ImportID == importID {
			out = append(out, *b)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// Imports returns all imports (oldest first).
func (m *Memory) Imports() []Import {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Import, len(m.imports))
	for i, imp := range m.imports {
		out[i] = *imp
	}
	return out
}

func (m *Memory) Claim(_ context.Context, owner string, tables []string, keep bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return err
	}
	want := map[string]bool{}
	for _, t := range tables {
		want[t] = true
	}
	owners := make([]string, 0, len(m.claims))
	for o := range m.claims {
		owners = append(owners, o)
	}
	sort.Strings(owners)
	for _, o := range owners {
		if o == owner {
			continue
		}
		var overlap []string
		for _, t := range m.claims[o] {
			if want[t] {
				overlap = append(overlap, t)
			}
		}
		if len(overlap) > 0 {
			sort.Strings(overlap)
			if len(overlap) > 10 {
				overlap = overlap[:10]
			}
			return &ClaimConflict{Owner: o, Tables: overlap, LastSeen: time.Now()}
		}
	}
	if keep {
		for _, t := range m.claims[owner] {
			want[t] = true
		}
	}
	list := make([]string, 0, len(want))
	for t := range want {
		list = append(list, t)
	}
	sort.Strings(list)
	m.claims[owner] = list
	return nil
}

// Memory models tables only as far as the engine's table operations go:
// Tables (when set) lists the existing ones; Dropped and Moved record the
// operations; OnMove lets a fake imposm follow its tables.

func (m *Memory) Claimed(_ context.Context, owner string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return nil, err
	}
	return append([]string(nil), m.claims[owner]...), nil
}

func (m *Memory) DropTables(_ context.Context, tables []string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return nil, err
	}
	kept := map[string]string{}
	for _, t := range tables {
		if reason, ok := m.Dependents[t]; ok {
			kept[t] = reason
			continue
		}
		if m.Tables != nil {
			if !m.Tables[t] {
				continue
			}
			delete(m.Tables, t)
		}
		m.Dropped = append(m.Dropped, t)
	}
	return kept, nil
}

func (m *Memory) MoveTables(_ context.Context, moves []TableMove) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return err
	}
	// as Postgres: only existing tables move; a target may be another moved
	// table's old name; anything else at a target is a conflict
	exists := func(t string) bool { return m.Tables == nil || m.Tables[t] }
	var todo []TableMove
	moving := map[string]bool{}
	for _, mv := range moves {
		if exists(mv.From) && mv.From != mv.To {
			todo = append(todo, mv)
			moving[mv.From] = true
		}
	}
	if m.Tables != nil {
		for _, mv := range todo {
			if m.Tables[mv.To] && !moving[mv.To] {
				return &TableExists{Table: mv.To}
			}
		}
		for _, mv := range todo {
			delete(m.Tables, mv.From)
		}
		for _, mv := range todo {
			m.Tables[mv.To] = true
		}
	}
	schemas := map[[2]string]bool{}
	for _, mv := range todo {
		m.Moved = append(m.Moved, mv)
		from, _ := SplitTable(mv.From)
		to, _ := SplitTable(mv.To)
		schemas[[2]string{from, to}] = true
	}
	if m.OnMove != nil {
		for pair := range schemas {
			m.OnMove(pair[0], pair[1])
		}
	}
	return nil
}
