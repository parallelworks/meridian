package workerapisrv

import (
	"context"

	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
)

// digest returns the digest that the server keeps of data, of the run, in
// data's place, as purpose and id, to tell a repeat of data from something
// else, where it must, as of a request that asks for an action once: an
// HMAC-SHA256 under a key that the run's data key derives
// (seal.Sealer.Digest), since data can hold a secret, which a plain digest
// would let whoever holds the store test a guess at. It is
// store.ErrNotFound for a run that has ended, whose data key is deleted
// with its record, so that a digest of it can't be checked again. Compare
// two with hmac.Equal.
func (s *Server) digest(ctx context.Context, run string, purpose seal.Purpose, id string, data []byte) ([]byte, error) {
	var wrapped []byte
	if err := s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		wrapped, err = tx.WrappedKey(run)
		return err
	}); err != nil {
		return nil, err
	}
	return s.sealer.Digest(ctx, run, wrapped, purpose, id, data)
}
