package client

import (
	"math/rand"
	"sort"
	"sync"
	"time"
)

type PieceManager struct {
	mu           sync.Mutex
	numPieces    int
	availability []int // frequency of each piece across connected peers
	
	todo    map[int]struct{} // Pieces that need to be downloaded
	pending map[int]struct{} // Pieces currently being downloaded
	done    map[int]struct{} // Pieces that have been successfully downloaded

	rng *rand.Rand
}

func NewPieceManager(numPieces int, initialBitfield []byte) *PieceManager {
	pm := &PieceManager{
		numPieces:    numPieces,
		availability: make([]int, numPieces),
		todo:         make(map[int]struct{}),
		pending:      make(map[int]struct{}),
		done:         make(map[int]struct{}),
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	for i := 0; i < numPieces; i++ {
		// If initialBitfield is provided, mark existing pieces as done
		if len(initialBitfield) > 0 && initialBitfield[i/8]&(1<<(i%8)) != 0 {
			pm.done[i] = struct{}{}
		} else {
			pm.todo[i] = struct{}{}
		}
	}

	return pm
}

// AddPeerAvailability increments the availability count for all pieces the peer has
func (pm *PieceManager) AddPeerAvailability(bitfield []byte) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for i := 0; i < pm.numPieces; i++ {
		if i/8 < len(bitfield) && bitfield[i/8]&(1<<(i%8)) != 0 {
			pm.availability[i]++
		}
	}
}

// UpdateAvailability increments the availability count for a single piece (Have message)
func (pm *PieceManager) UpdateAvailability(pieceIndex int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pieceIndex >= 0 && pieceIndex < pm.numPieces {
		pm.availability[pieceIndex]++
	}
}

// NextPiece returns the index of the next best piece to download based on the peer's bitfield using Rarest First.
func (pm *PieceManager) NextPiece(peerBitfield []byte) (int, bool) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	var availableTodo []int

	// Find pieces that are in todo and the peer has them
	for pieceIndex := range pm.todo {
		if pieceIndex/8 < len(peerBitfield) && peerBitfield[pieceIndex/8]&(1<<(pieceIndex%8)) != 0 {
			availableTodo = append(availableTodo, pieceIndex)
		}
	}

	if len(availableTodo) == 0 {
		// Endgame mode: if all pieces are pending or done, assign a pending piece
		// that the peer has, to speed up the tail.
		var availablePending []int
		for pieceIndex := range pm.pending {
			if pieceIndex/8 < len(peerBitfield) && peerBitfield[pieceIndex/8]&(1<<(pieceIndex%8)) != 0 {
				availablePending = append(availablePending, pieceIndex)
			}
		}

		if len(availablePending) == 0 {
			return 0, false
		}

		// Pick a random pending piece
		bestPiece := availablePending[pm.rng.Intn(len(availablePending))]
		return bestPiece, true
	}

	// Sort by availability (Rarest First)
	sort.Slice(availableTodo, func(i, j int) bool {
		idxI, idxJ := availableTodo[i], availableTodo[j]
		if pm.availability[idxI] == pm.availability[idxJ] {
			// Randomize if same availability to distribute work
			return pm.rng.Intn(2) == 0
		}
		return pm.availability[idxI] < pm.availability[idxJ]
	})

	bestPiece := availableTodo[0]
	delete(pm.todo, bestPiece)
	pm.pending[bestPiece] = struct{}{}
	return bestPiece, true
}

// SetDone marks a piece as successfully downloaded
func (pm *PieceManager) SetDone(pieceIndex int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.pending, pieceIndex)
	pm.done[pieceIndex] = struct{}{}
}

// SetFailed moves a piece from pending back to todo
func (pm *PieceManager) SetFailed(pieceIndex int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.pending, pieceIndex)
	if _, isDone := pm.done[pieceIndex]; !isDone {
		pm.todo[pieceIndex] = struct{}{}
	}
}
