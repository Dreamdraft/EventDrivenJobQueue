package jobqueue

import (
	"context"
	"database/sql"
	"log"
	"time"
)

func StartDispatcher(db *sql.DB, ctx context.Context, workerCh chan WorkerJob) {
	defer close(workerCh) //close the dispatcher and worker communication
	// pull the job from the db and assign to the worker
	for {
		select {
		case <-ctx.Done():
			log.Println("Dispatcher is Dead")
			return
		default:
		}
		job, err := ClaimJob(db)
		if err == sql.ErrNoRows {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if err != nil {
			log.Println("dispatcher error:", err)
			time.Sleep(time.Second)
			continue
		}
		select {
		case workerCh <- job:
		case <-ctx.Done():
			log.Println("Dispatcher is Dead")
			return
		}
	}
}
