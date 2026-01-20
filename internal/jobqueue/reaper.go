package jobqueue

import (
	"context"
	"database/sql"
	"log"
	"time"
)

func StartVisibilityReaper(db *sql.DB, ctx context.Context) {
	timer := time.NewTicker(30 * time.Second)
	go func() {
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				_, err := db.ExecContext(ctx, `UPDATE jobs 
			SET
 
			status = CASE
			WHEN attempts < max_retries THEN 'queued'
			ELSE 'failed'
		    END, 

			started_at = CASE
			WHEN attempts < max_retries THEN NULL
			ELSE started_at
			END,

			finished_at = CASE
            WHEN attempts >= max_retries THEN CURRENT_TIMESTAMP
            ELSE finished_at
            END

			WHERE status = 'processing' AND started_at < datetime('now','-5 minutes')`)
				if err != nil {
					log.Println("visibility reaper error:", err)
				}
			case <-ctx.Done():
				log.Println("Visibility reaper stopped")
				return
			}
		}
	}()
}
