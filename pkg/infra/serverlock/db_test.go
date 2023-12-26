package serverlock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/grafana/pkg/infra/db"
	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/infra/tracing"
)

func createTestableServerLockDB(t *testing.T) *serverLockDB {
	t.Helper()

	store := db.InitTestDB(t)

	return &serverLockDB{
		SQLStore: store,
		tracer:   tracing.InitializeTracerForTest(),
		log:      log.New("test-logger"),
	}
}

func TestLockAndRelease(t *testing.T) {
	operationUID := "test-operation-release"

	t.Run("create lock and then release it", func(t *testing.T) {
		sl := createTestableServerLockDB(t)
		duration := time.Hour * 5

		err := sl.AcquireForRelease(context.Background(), operationUID, duration)
		require.NoError(t, err)

		err = sl.ReleaseLock(context.Background(), operationUID)
		require.NoError(t, err)

		// and now we can acquire it again
		err2 := sl.AcquireForRelease(context.Background(), operationUID, duration)
		require.NoError(t, err2)

		err = sl.ReleaseLock(context.Background(), operationUID)
		require.NoError(t, err)
	})

	t.Run("try to acquire a lock which is already locked, get error", func(t *testing.T) {
		sl := createTestableServerLockDB(t)
		duration := time.Hour * 5

		err := sl.AcquireForRelease(context.Background(), operationUID, duration)
		require.NoError(t, err)

		err2 := sl.AcquireForRelease(context.Background(), operationUID, duration)
		require.Error(t, err2, "We should expect an error when trying to get the second lock")
		require.Equal(t, "there is already a lock for this actionName: "+operationUID, err2.Error())

		err3 := sl.ReleaseLock(context.Background(), operationUID)
		require.NoError(t, err3)
	})

	t.Run("lock already exists but is timeouted", func(t *testing.T) {
		sl := createTestableServerLockDB(t)
		pastLastExec := time.Now().Add(-time.Hour).Unix()
		lock := serverLock{
			OperationUID:  operationUID,
			LastExecution: pastLastExec,
		}

		// inserting a row with lock in the past
		err := sl.SQLStore.WithTransactionalDbSession(context.Background(), func(sess *db.Session) error {
			affectedRows, err := sess.Insert(&lock)
			require.NoError(t, err)
			require.Equal(t, int64(1), affectedRows)
			require.Equal(t, int64(1), lock.Id)
			return nil
		})
		require.NoError(t, err)
		duration := time.Minute * 5

		err = sl.AcquireForRelease(context.Background(), operationUID, duration)
		require.NoError(t, err)

		//validate that the lock LastExecution was updated (at least different from the original)
		err = sl.SQLStore.WithTransactionalDbSession(context.Background(), func(sess *db.Session) error {
			lockRows := []*serverLock{}
			err := sess.Where("operation_uid = ?", operationUID).Find(&lockRows)
			require.NoError(t, err)
			require.Equal(t, 1, len(lockRows))
			require.NotEqual(t, pastLastExec, lockRows[0].LastExecution)
			return nil
		})
		require.NoError(t, err)

		err3 := sl.ReleaseLock(context.Background(), operationUID)
		require.NoError(t, err3)
	})
}
