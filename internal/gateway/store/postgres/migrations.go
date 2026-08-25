package postgres

import "embed"

// Migrations contains the forward-only PostgreSQL schema owned by this package.
// Callers apply files in lexical order with their deployment migration runner.
// Each file is one indivisible unit: execute its complete bytes on one session,
// do not split statements, and do not add an outer transaction. Migrations that
// temporarily relax FORCE RLS own explicit BEGIN/COMMIT boundaries so any
// injected or operational failure rolls the complete boundary back.
//
//go:embed migrations/*.sql
var Migrations embed.FS
