package scheduler

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"
)

// PersistentBudget serializes each domain across instances with a PostgreSQL
// session advisory lock and a bounded durable lease. If the connection dies,
// the lease continues to block other instances until the request deadline plus
// a safety margin. Callers must use the same context for the network request
// and stop it when the context expires. The conservative per-domain limit is one.
type PersistentBudget struct {
	DB       *sql.DB
	Local    *Budget
	Domain   string
	Interval time.Duration
}

func (p PersistentBudget) Acquire(ctx context.Context) (func(), error) {
	if p.DB == nil || p.Local == nil || p.Domain == "" || p.Domain != strings.ToLower(p.Domain) || p.Interval < time.Microsecond {
		return nil, errors.New("invalid persistent budget")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 2*time.Minute {
		return nil, errors.New("request deadline must be within two minutes")
	}
	var tokenBytes [16]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		return nil, err
	}
	token := hex.EncodeToString(tokenBytes[:])
	reserved := false
	localRelease, err := p.Local.Acquire(ctx, p.Domain, time.Nanosecond, 1)
	if err != nil {
		return nil, err
	}
	conn, err := p.DB.Conn(ctx)
	if err != nil {
		localRelease()
		return nil, err
	}
	locked := false
	cleanup := func() {
		if locked {
			unlockCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if reserved {
				_, _ = conn.ExecContext(unlockCtx, `UPDATE source_domain_budget SET active_until='-infinity',active_token=NULL WHERE domain=$1 AND active_token=$2`, p.Domain, token)
			}
			var ok bool
			if err := conn.QueryRowContext(unlockCtx, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, "source-budget/"+p.Domain).Scan(&ok); err != nil || !ok {
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
		}
		_ = conn.Close()
		localRelease()
	}
	for {
		var ok bool
		err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,0))`, "source-budget/"+p.Domain).Scan(&ok)
		if err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			cleanup()
			return nil, err
		}
		if ok {
			locked = true
			break
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			cleanup()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO source_domain_budget(domain) VALUES($1) ON CONFLICT DO NOTHING`, p.Domain)
	if err != nil {
		cleanup()
		return nil, err
	}
	for {
		var waitSeconds float64
		err = conn.QueryRowContext(ctx, `SELECT CASE WHEN GREATEST(next_allowed_at,cooldown_until,active_until)>clock_timestamp() THEN EXTRACT(EPOCH FROM (GREATEST(next_allowed_at,cooldown_until,active_until)-clock_timestamp())) ELSE 0 END FROM source_domain_budget WHERE domain=$1`, p.Domain).Scan(&waitSeconds)
		if err != nil {
			cleanup()
			return nil, err
		}
		if waitSeconds <= 0 {
			break
		}
		timer := time.NewTimer(time.Duration(waitSeconds * float64(time.Second)))
		select {
		case <-ctx.Done():
			timer.Stop()
			cleanup()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	_, err = conn.ExecContext(ctx, `UPDATE source_domain_budget SET next_allowed_at=clock_timestamp()+($2 * interval '1 microsecond'),active_until=clock_timestamp()+($3 * interval '1 microsecond'),active_token=$4 WHERE domain=$1`, p.Domain, p.Interval.Microseconds(), (time.Until(deadline) + time.Second).Microseconds(), token)
	if err != nil {
		cleanup()
		return nil, err
	}
	reserved = true
	var once sync.Once
	return func() { once.Do(cleanup) }, nil
}

// Cooldown extends, never shortens, the persistent domain cooldown.
func (p PersistentBudget) Cooldown(ctx context.Context, until time.Time) error {
	if p.DB == nil || p.Domain == "" {
		return errors.New("invalid persistent budget")
	}
	_, err := p.DB.ExecContext(ctx, `INSERT INTO source_domain_budget(domain,cooldown_until) VALUES($1,$2) ON CONFLICT(domain) DO UPDATE SET cooldown_until=GREATEST(source_domain_budget.cooldown_until,EXCLUDED.cooldown_until)`, p.Domain, until)
	return err
}
