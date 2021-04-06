package sync

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/data"
)

type Src struct {
	// srcLastTS is the time corresponding to the latest record in the source
	lastTS time.Time
	// ctx is the context passed to gRPC Calls
	// client is the connection to the source host
	client *data.GrpcClient
}

type Dst struct {
	// dstLastTS is the time corresponding to the latest record in the destionation
	lastTS time.Time
	// client is the connection to the destination host
	client *data.GrpcClient
}

type Synchronizer struct {
	// series to by synced
	series string
	// firstTS holds the starting time from which sync needs to start
	firstTS time.Time
	// interval in which the synchronization should happen. when 0, the synchronization will be continuous
	interval time.Duration
	// src holds the information related to the source series
	src Src
	//dst holds the information related to the destination series
	dst Dst
	// ctx is the context passed to gRPC Calls
	ctx context.Context
	// cancel function to cancel any of the running gRPC communication whenever the synchronization needs to be stopped
	cancel context.CancelFunc
}

func newSynchronization(series string, srcClient *data.GrpcClient, dstClient *data.GrpcClient, interval time.Duration) (s *Synchronizer) {
	zeroTime := time.Time{}

	s = &Synchronizer{
		series:   series,
		firstTS:  zeroTime, //TODO: This should come as an argument.
		interval: interval,
		src: Src{
			lastTS: zeroTime,
			client: srcClient,
		},
		dst: Dst{
			lastTS: zeroTime,
			client: dstClient,
		},
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	go s.synchronize()
	return s
}

// clear ensures graceful shutdown of the synchronization related to the series
func (s *Synchronizer) clear() {
	s.cancel()
}

func (s *Synchronizer) synchronize() {
	canceled := false
	if s.interval == 0 {
		for !canceled {
			s.subscribeAndPublish()
			canceled = sleepContext(s.ctx, time.Second)
		}
	} else {
		for !canceled {
			s.periodicSynchronization()
			canceled = sleepContext(s.ctx, s.interval)
		}
	}
}

func (s *Synchronizer) subscribeAndPublish() {
	// get the latest measurement from source

	var err error
	s.src.lastTS, err = getLastTime(s.ctx, s.src.client, s.series, time.Time{}, time.Now())
	if err != nil {
		log.Printf("%s: failed to get latest measurement at source: %v", s.series, err)
		return
	}

	s.dst.lastTS, err = getLastTime(s.ctx, s.dst.client, s.series, time.Time{}, time.Now())
	if err != nil {
		log.Printf("%s: failed to get latest measurement at destination:%v", s.series, err)
		return
	}
	//subscribe to source HDS
	responseCh, err := s.src.client.Subscribe(s.ctx, s.series)
	log.Printf("Success subscribing to source %s", s.series)
	if err != nil {
		log.Printf("%s: error subscribing to source: %v", s.series, err)
		return
	}

	backfillDoneCh := make(chan struct{})
	if s.dst.lastTS.Before(s.src.lastTS) {
		log.Printf("%s: src and destination time (%v vs %v) do not match. starting migrate", s.series, s.src.lastTS, s.dst.lastTS)
		go s.backfill(s.dst.lastTS, s.src.lastTS, backfillDoneCh)
	} else {
		close(backfillDoneCh)
	}
	var buffer senml.Pack
	for response := range responseCh {
		if response.Err != nil {
			log.Printf("%s: error recieving stream: %v", s.series, response.Err)
			return
		}
		pack := response.Pack
		latestInPack := getLatestInPack(pack) // get latest in the pack
		log.Printf("%s: src latest:%v, dest latest:%v, latestinpack %v", s.series, s.src.lastTS, s.dst.lastTS, latestInPack)
		buffer = append(buffer, pack...)
		select {
		case <-backfillDoneCh:
			//buffer = append(buffer, pack...)
			err = s.dst.client.Submit(s.ctx, buffer)
			if err != nil {
				log.Printf("%s: error copying entries : %v", s.series, err)
				return
			} else {
				log.Printf("%s: migrated SenML pack of len %d", s.series, len(pack))
				s.dst.lastTS = latestInPack
			}
			buffer = nil

			s.src.lastTS = latestInPack
		default:
			log.Printf("%s: buffering %d records", s.series, len(pack))
		}

	}

}

func (s *Synchronizer) periodicSynchronization() {
	var err error
	s.src.lastTS, err = getLastTime(s.ctx, s.src.client, s.series, time.Time{}, time.Now())
	if err != nil {
		log.Printf("%s: failed to get latest measurement at source: %v", s.series, err)
		return
	}

	s.dst.lastTS, err = getLastTime(s.ctx, s.dst.client, s.series, time.Time{}, time.Now())
	if err != nil {
		log.Printf("%s: failed to get latest measurement at dest: %v", s.series, err)
		return
	}

	log.Printf("%s: periodicSync: src latest :%v, dest latest: %v", s.series, s.src.lastTS, s.dst.lastTS)
	if s.src.lastTS.After(s.dst.lastTS) {
		s.migrate(s.dst.lastTS, s.src.lastTS)
	}

}

func getLastTime(ctx context.Context, client *data.GrpcClient, series string, from time.Time, to time.Time) (time.Time, error) {
	pack, err := client.Query(ctx, []string{series}, data.Query{From: from, To: to, Limit: 1, SortAsc: false})
	if err != nil {
		return time.Time{}, fmt.Errorf("series:%s, error:%s", series, err)
	}
	if len(pack) != 1 {
		return to, nil
	}
	return data.FromSenmlTime(pack[0].Time), err
}

func (s *Synchronizer) backfill(from time.Time, to time.Time, backfillDoneCh chan struct{}) {
	defer close(backfillDoneCh)
	s.migrate(from, to)
}
func (s *Synchronizer) migrate(from time.Time, to time.Time) {
	adjustment := time.Microsecond
	from = from.Add(adjustment)
	to = to.Add(adjustment) //add little delay to `to` inorder to avoid missing the latest measurements because of floating point errors

	log.Printf("%s: starting migrate from %v to %v", s.series, from, to)
	ctx := s.ctx
	destStream, err := s.dst.client.CreateSubmitStream(ctx)
	if err != nil {
		log.Printf("%s: error getting the stream: %v", s.series, err)
	}

	defer s.dst.client.CloseSubmitStream(destStream)
	//get last time from Src HDS
	q := data.Query{
		Denormalize: data.DenormMaskName | data.DenormMaskTime | data.DenormMaskUnit,
		SortAsc:     true,
		From:        from,
		To:          to, //add a second to to to avoid missing the latest measurements because of floating point errors
	}
	sourceChannel, err := s.src.client.QueryStream(ctx, []string{s.series}, q)
	if err != nil {
		log.Printf("%s: error querying the source: %v", s.series, err)
		return
	}
	totalSynced := 0
	for response := range sourceChannel {
		if response.Err != nil {
			log.Printf("%s: migrate aborted: error recieving stream : %v", s.series, response.Err)
			break
		}
		err = s.dst.client.SubmitToStream(destStream, response.Pack)
		if err != nil {
			log.Printf("%s: migrate aborted: error submitting stream %s: %v", s.series, err)
			break
		}
		s.dst.lastTS = getLatestInPack(response.Pack)
		totalSynced += len(response.Pack)

	}
	log.Printf("%s: migrated %d entries. dest latest: %v", s.series, totalSynced, s.dst.lastTS)

}

func getLatestInPack(pack senml.Pack) time.Time {
	//Since it is not assured that the pack will be sorted, we search exhaustively to find the latest
	bt := pack[0].BaseTime
	latestInPack := bt + pack[0].Time
	for _, r := range pack {
		t := bt + r.Time
		if t > latestInPack {
			latestInPack = t
		}
	}
	return data.FromSenmlTime(latestInPack)
}

func sleepContext(ctx context.Context, delay time.Duration) (cancelled bool) {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(delay):
		return false
	}
}
