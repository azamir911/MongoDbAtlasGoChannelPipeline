package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/rs/zerolog"
	"os"
	"time"

	//"log"
	"math/big"
	"math/rand"
	"sync"
)

const (
	cyLogger = "cylogger"
)

func RandomGenerator() <-chan uint64 {
	c := make(chan uint64)
	go func() {
		rnds := make([]byte, 8)
		for {
			_, err := rand.Read(rnds)
			if err != nil {
				close(c)
				break
			}
			c <- binary.BigEndian.Uint64(rnds)
		}
	}()
	return c
}

func Calculator(in <-chan uint64, out chan uint64) <-chan uint64 {
	if out == nil {
		out = make(chan uint64)
	}
	go func() {
		for x := range in {
			out <- ^x
		}
	}()
	return out
}

func Filter0(input <-chan uint64, output chan uint64) <-chan uint64 {
	if output == nil {
		output = make(chan uint64)
	}
	go func() {
		bigInt := big.NewInt(0)
		for x := range input {
			bigInt.SetUint64(x)
			if bigInt.ProbablyPrime(1) {
				output <- x
			}
		}
	}()
	return output
}

func Filter(input <-chan uint64) <-chan uint64 {
	return Filter0(input, nil)
}

func Printer(input <-chan uint64) {
	for x := range input {
		fmt.Println(x)
	}
}

func main() {
	//Printer(
	//	Filter(
	//		Calculator(
	//			RandomGenerator(), nil,
	//		),
	//	),
	//)

	client := Client()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	log := zerolog.New(output).With().Timestamp().Logger()
	ctx, _ := context.WithCancel(context.WithValue(context.Background(), cyLogger, &log))

	var wg sync.WaitGroup

	//																									/ OrganizationPrinter
	// OrganizationsStreamer -> OrganizationsFilter -> OrganizationsMapper -> OrganizationsDuplicator -|-> AtlasUsersStreamer
	// 																									\ TeamsStreamer
	organizationsChA, organizationsCnB, organizationsCnC := OrganizationsDuplicator(
		ctx, &wg, OrganizationsMapper(
			ctx, &wg, OrganizationsFilter(
				ctx, &wg, OrganizationsStreamer(
					ctx, &wg, client,
				),
			),
		),
	)
	OrganizationPrinter(ctx, &wg, organizationsChA)

	// AtlasUsersStreamer -> AtlasUsersResponseMapper -> AtlasUsersFilter -> AtlasUserPrinter
	AtlasUserPrinter(
		ctx, &wg, AtlasUsersFilter(
			ctx, &wg, AtlasUsersResponseMapper(
				ctx, &wg, AtlasUsersStreamer(
					ctx, &wg, client, organizationsCnB,
				),
			),
		),
	)

	// TeamsStreamer -> TeamsMapper -> TeamFilter -> TeamPrinter
	TeamPrinter(
		ctx, &wg, TeamFilter(
			ctx, &wg, TeamsMapper(
				ctx, &wg, TeamsStreamer(
					ctx, &wg, client, organizationsCnC,
				),
			),
		),
	)

	// 																							    / ProjectPrinter
	// ProjectsStreamer -> ProjectsFilter -> ProjectsMapper -> ProjectFilter -> ProjectDuplicator -|-> ClustersWithTeamsStreamer
	//																							   |\ DatabaseUsersStreamer
	//																							    \ CustomDbRolesStreamer
	projectsCnA, projectsCnB, projectsCnC, projectsCnD := ProjectDuplicator(
		ctx, &wg, ProjectFilter(
			ctx, &wg, ProjectsMapper(
				ctx, &wg, ProjectsFilter(
					ctx, &wg, ProjectsStreamer(
						ctx, &wg, client,
					),
				),
			),
		),
	)
	ProjectPrinter(ctx, &wg, projectsCnA)

	// 																					   							/ ClusterWithTeamsPrinter
	// TeamsAssignedStreamer -> ClustersWithTeamsStreamer -> ClustersWithTeamsMapper -> ClusterWithTeamsDuplicator |- ClusterWithTeamsMapper
	clusterWithTeamsCnA, clusterWithTeamsCnB := ClusterWithTeamsDuplicator(
		ctx, &wg, ClustersWithTeamsMapper(
			ctx, &wg, ClustersWithTeamsStreamer(
				ctx, &wg, client, TeamsAssignedStreamer(
					ctx, &wg, client, projectsCnB,
				),
			),
		),
	)
	ClusterWithTeamsPrinter(ctx, &wg, clusterWithTeamsCnA)

	// 											    / SnapshotsStreamer
	// ClusterWithTeamsMapper -> ClusterDuplicator |- SnapshotsStreamer
	//											    \ SnapshotsRestoreJobsStreamer

	clusterCnA, clusterCnB, clusterCnC := ClusterDuplicator(
		ctx, &wg, ClusterWithTeamsMapper(
			ctx, &wg, clusterWithTeamsCnB,
		),
	)

	// DatabaseUsersStreamer -> DatabaseUsersMapper -> DatabaseUserFilter -> DatabaseUserPrinter
	DatabaseUserPrinter(
		ctx, &wg, DatabaseUserFilter(
			ctx, &wg, DatabaseUsersMapper(
				ctx, &wg, DatabaseUsersStreamer(
					ctx, &wg, client, projectsCnC,
				),
			),
		),
	)

	// CustomDbRolesStreamer -> CustomDbRolesMapper -> CustomDbRoleFilter -> CustomDbRolePrinter
	CustomDbRolePrinter(
		ctx, &wg, CustomDbRoleFilter(
			ctx, &wg, CustomDbRolesMapper(
				ctx, &wg, CustomDbRolesStreamer(
					ctx, &wg, client, projectsCnD,
				),
			),
		),
	)

	// SnapshotsStreamer1 \
	//						| -> SnapshotsAggregator -> SnapshotsMapper -> SnapshotFilter -> SnapshotPrinter
	// SnapshotsStreamer2 /
	streamer1 := SnapshotsStreamer(
		ctx, &wg, client, clusterCnA, 1,
	)
	streamer2 := SnapshotsStreamer(
		ctx, &wg, client, clusterCnB, 2,
	)
	SnapshotPrinter(
		ctx, &wg, SnapshotFilter(
			ctx, &wg, SnapshotsMapper(
				ctx, &wg, SnapshotsAggregator(
					ctx, &wg, streamer1, streamer2,
				),
			),
		),
	)

	// SnapshotsRestoreJobsStreamer -> SnapshotsRestoreJobsMapper -> SnapshotRestoreJobFilter -> SnapshotRestoreJobPrinter
	SnapshotRestoreJobPrinter(
		ctx, &wg, SnapshotRestoreJobFilter(
			ctx, &wg, SnapshotsRestoreJobsMapper(
				ctx, &wg, SnapshotsRestoreJobsStreamer(
					ctx, &wg, client, clusterCnC,
				),
			),
		),
	)

	//time.Sleep(time.Second * 5)
	//cancelFunc()

	wg.Wait()
}

////////////////////// Channel Encapsulated in Channel //////////////////////
/*
var counter = func(n int) chan<- chan<- int {
	requests := make(chan chan<- int)
	go func() {
		for request := range requests {
			if request == nil {
				n++ // increase
			} else {
				request <- n // take out
			}
		}
	}()

	// Implicitly converted to chan<- (chan<- int)
	return requests
}(0)

var increase1000 = func(done chan<- struct{}) {
	for i := 0; i < 100; i++ {
		counter <- nil
	}
	done <- struct{}{}
}

func main() {

	done := make(chan struct{})
	go increase1000(done)
	go increase1000(done)
	<-done
	<-done

	request := make(chan int, 1)
	counter <- request
	fmt.Println(<-request) // 2000
}
*/

////////////////////// Dialogue (Ping-Pong) //////////////////////
/*
type Ball uint64

func Play(playerName string, table chan Ball) {
	var lastValue Ball = 1
	for {
		ball := <-table // get the ball
		fmt.Println(playerName, ball)
		ball += lastValue
		if ball < lastValue { // overflow
			os.Exit(0)
		}
		lastValue = ball
		table <- ball // bat back the ball
		time.Sleep(time.Second)
	}
}

func main() {
	table := make(chan Ball)
	go func() {
		table <- 1 // throw ball on table
	}()
	go Play("A:", table)
	Play("B:", table)
}
*/

////////////////////// Use Channels as Counting Semaphores //////////////////////
/*
type Seat int
type Bar chan Seat

func (bar Bar) ServeCustomer(c int) {
	log.Print("customer#", c, " enters the bar")
	seat := <-bar // need a seat to drink
	log.Print("++ customer#", c, " drinks at seat#", seat)
	time.Sleep(time.Second * time.Duration(2+rand.Intn(6)))
	log.Print("-- customer#", c, " frees seat#", seat)
	bar <- seat // free seat and leave the bar
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// the bar has 10 seats.
	bar24x7 := make(Bar, 10)
	// Place seats in an bar.
	for seatId := 0; seatId < cap(bar24x7); seatId++ {
		// None of the sends will block.
		bar24x7 <- Seat(seatId)
	}

	for customerId := 0; ; customerId++ {
		time.Sleep(time.Second)
		go bar24x7.ServeCustomer(customerId)
	}

	// sleeping != blocking
	for {
		time.Sleep(time.Second)
	}
}
*/

////////////////////// Timer: scheduled notification //////////////////////
/*
func AfterDuration(d time.Duration) <-chan struct{} {
	c := make(chan struct{}, 1)
	go func() {
		time.Sleep(d)
		c <- struct{}{}
	}()
	return c
}

func main() {
	fmt.Println("Hi!")
	<-AfterDuration(time.Second)
	fmt.Println("Hello!")
	<-AfterDuration(time.Second)
	fmt.Println("Bye!")
}
*/

////////////////////// N-to-1 and 1-to-N notifications //////////////////////
/*
type T = struct{}

func worker(id int, ready <-chan T, done chan<- T) {
	<-ready // block here and wait a notification
	log.Print("Worker#", id, " starts.")
	// Simulate a workload.
	time.Sleep(time.Second * time.Duration(id+1))
	log.Print("Worker#", id, " job done.")
	// Notify the main goroutine (N-to-1),
	done <- T{}
}

func main() {
	log.SetFlags(0)

	ready, done := make(chan T), make(chan T)
	go worker(0, ready, done)
	go worker(1, ready, done)
	go worker(2, ready, done)

	// Simulate an initialization phase.
	time.Sleep(time.Second * 3 / 2)
	// 1-to-N notifications.
	//ready <- T{}
	//ready <- T{}
	//ready <- T{}
	close(ready)
	// Being N-to-1 notified.
	<-done
	<-done
	<-done
}
*/

////////////////////// 1-to-1 notification by sending a value to a channel //////////////////////

/*
func main() {
	values := make([]byte, 32*1024*1024)
	if _, err := rand.Read(values); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	done := make(chan struct{}) // can be buffered or not

	// The sorting goroutine
	go func() {
		sort.Slice(values, func(i, j int) bool {
			return values[i] < values[j]
		})
		// Notify sorting is done.
		done <- struct{}{}
	}()

	// do some other things ...

	<-done // waiting here for notification
	fmt.Println(values[0], values[len(values)-1])
}
*/
