package check

import (
	"context"
	"sync"

	"github.com/ngaut/log"
)

// Checker is interface that defines checker to check configurations of system.
// It is mainly used for configuration checking of data synchronization between database systems.
type Checker interface {
	Name() string
	Check(ctx context.Context) *Result
}

// State is state of check
type State string

const (
	// StateSuccess indicates that the check was successful
	StateSuccess State = "success"
	// StateFailure indicates that the check was failed
	StateFailure State = "fail"
	// StateWarning indicates that the check had warnings
	StateWarning State = "warn"
)

// Result is result of check
type Result struct {
	ID          uint64 `json:"id"`
	Name        string `json:"name"`
	Desc        string `json:"desc"`
	State       State  `json:"state"`
	ErrorMsg    string `json:"errorMsg"`
	Instruction string `json:"instruction"`
	Extra       string `json:"extra"`
}

// ResultSummary is summary of all check results
type ResultSummary struct {
	Passed     bool  `json:"passed"`
	Total      int64 `json:"total"`
	Successful int64 `json:"successful"`
	Failed     int64 `json:"failed"`
	Warning    int64 `json:"warning"`
}

// Results contains all check results and summary
type Results struct {
	Results []*Result      `json:"results"`
	Summary *ResultSummary `json:"summary"`
}

// Do executes several checkers.
func Do(ctx context.Context, checkers []Checker) (*Results, error) {
	results := &Results{
		Results: make([]*Result, 0, len(checkers)),
	}
	if len(checkers) == 0 {
		results.Summary = &ResultSummary{Passed: true}
		return results, nil
	}

	var (
		wg         sync.WaitGroup
		finished   bool
		total      int64
		successful int64
		failed     int64
		warning    int64
	)
	total = int64(len(checkers))

	resultCh := make(chan *Result)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case result := <-resultCh:
				switch result.State {
				case StateSuccess:
					successful++
				case StateFailure:
					failed++
				case StateWarning:
					warning++
				}

				// if total == successful + warning + failed, it's finished
				finished = (total == successful+warning+failed)
				results.Results = append(results.Results, result)

				log.Debugf("check result:%+v", result)
				if finished {
					return
				}
			}
		}
	}()

	for i, checker := range checkers {
		wg.Add(1)
		go func(i int, checker Checker) {
			defer wg.Done()
			result := checker.Check(ctx)
			result.ID = uint64(i)
			resultCh <- result
		}(i, checker)
	}
	wg.Wait()

	passed := finished && (failed == 0)
	results.Summary = &ResultSummary{
		Passed:     passed,
		Total:      total,
		Successful: successful,
		Failed:     failed,
		Warning:    warning,
	}

	log.Infof("check finished, passed %v / total %v", passed, total)
	return results, nil
}
