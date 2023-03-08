package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)

	SJFPrioritySchedule(os.Stdout, "Priority", processes)

	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			// how long the task waited since arrival
			waitingTime = serviceTime - processes[i].ArrivalTime // skipped 1, (5 - 3 = 2),
		}
		//total amount of wait time for every task
		totalWait += float64(waitingTime) // 0, (0 + 2 = 2),

		// time the task started
		start := waitingTime + processes[i].ArrivalTime // 0, (2 + 3 = 5),

		// how long it took to complete the task
		turnaround := processes[i].BurstDuration + waitingTime // 5, (9 + 2 = 11)

		// total amount of time taken to complete every task
		totalTurnaround += float64(turnaround) // 5, (5 + 11 = 16)

		// total time to complete tasks
		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime // 5, (9 + 3 + 2 = 14)

		lastCompletion = float64(completion) // 5, 14
		// fmt.Printf("completion: %v \n lastCompletion: %v\n\n", completion, lastCompletion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}

		// total time to complete tasks
		serviceTime += processes[i].BurstDuration // 5, (5 + 9 = 14),

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes)) // 3
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		schedule        = make([][]string, 0)
		gantt           = make([]TimeSlice, 0)
		timeline        = make([]int64, 0)
		complete        int
		currentTime     int64
		minValue        int64 = math.MaxInt64
		burstValues           = make([]int64, 0)
		index           int
	)

	// extract burst dur values
	for i := range processes {
		burstValues = append(burstValues, processes[i].BurstDuration)
	}

	for complete != len(processes) {
		for i := range processes {
			// arrival time must be less than or equal to current time, priority must be less than current minimum priority, and burst dur must be greater than 0
			if processes[i].ArrivalTime <= currentTime && processes[i].Priority < minValue && burstValues[i] > 0 {
				minValue = processes[i].Priority // found smallest priority
				index = i
			} else if processes[i].ArrivalTime <= currentTime && processes[i].Priority == minValue && burstValues[i] > 0 { // same priority found, check arrival times
				if processes[i].ArrivalTime < processes[index].ArrivalTime { // get priority of process which came first (FCFS)
					minValue = processes[i].Priority
					index = i
				}
			}
		}

		burstValues[index] -= 1
		// used to track which processes run in what order and for how long
		// add index of the process
		timeline = append(timeline, int64(index))

		minValue = burstValues[index]

		if minValue == 0 {
			minValue = math.MaxInt64 // process has finished, reset minValue
		}

		// process has finished
		if burstValues[index] == 0 {
			complete++ // increase number of processes completed
		}
		currentTime++
	}

	totalWait, totalTurnaround, lastCompletion = getValues(timeline, processes, &schedule, &gantt)

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		schedule        = make([][]string, 0)
		gantt           = make([]TimeSlice, 0)
		timeline        = make([]int64, 0)
		complete        int
		currentTime     int64
		minValue        int64 = math.MaxInt64
		burstValues           = make([]int64, 0)
		index           int
	)

	// extract burst duration values
	for i := range processes {
		burstValues = append(burstValues, processes[i].BurstDuration)
	}

	for complete != len(processes) {
		for i := range processes {
			// arrival time must be less than or equal to current time, burst dur must be less than current minimum burst dur, and burst dur must be greater than 0
			if processes[i].ArrivalTime <= currentTime && burstValues[i] < minValue && burstValues[i] > 0 {
				minValue = burstValues[i] // found smallest burst dur
				index = i
			} else if processes[i].ArrivalTime <= currentTime && burstValues[i] == minValue && burstValues[i] > 0 { // same burst duration found, check arrival times
				if processes[i].ArrivalTime < processes[index].ArrivalTime { // get burst value of process which came first (FCFS)
					minValue = burstValues[i]
					index = i
				}
			}
		}

		burstValues[index] -= 1
		// used to track which processes run in what order and for how long
		// add index of the process
		timeline = append(timeline, int64(index))

		minValue = burstValues[index]

		if minValue == 0 {
			minValue = math.MaxInt64 // process has finished, reset minValue
		}

		// process has finished
		if burstValues[index] == 0 {
			complete++ // increase number of processes completed
		}
		currentTime++
	}

	totalWait, totalTurnaround, lastCompletion = getValues(timeline, processes, &schedule, &gantt)

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		sortedProcesses       = make([]Process, 0)
		queue                 = make([]Process, 0)
		timeQuantum     int64 = 1
		head            Process
		currentTime     int64 = 1
		timeline              = make([]int64, 0)
		totalWait       float64
		totalTurnaround float64
		schedule        = make([][]string, 0)
		gantt           = make([]TimeSlice, 0)
		lastCompletion  float64
	)

	// copy processes
	sortedProcesses = append(sortedProcesses, processes...)
	// sort processes by arrival time
	sort.Slice(sortedProcesses, func(i, j int) bool {
		return sortedProcesses[i].ArrivalTime < sortedProcesses[j].ArrivalTime
	})

	queue = append(queue, sortedProcesses[0]) // first processes enters queue to run

	for len(queue) != 0 {
		// check if new processes have arrived
		if currentTime <= sortedProcesses[len(sortedProcesses)-1].ArrivalTime { // only run if current time is <= the arrival time of last process
			for i := 1; i < len(sortedProcesses); i++ {
				if sortedProcesses[i].ArrivalTime == currentTime { // process has arrived and entered into queue
					queue = append(queue, sortedProcesses[i])
				}
			}
		}
		// if no new processes have entered
		head = queue[0]
		queue = queue[1:]                             // remove first process from queue
		timeline = append(timeline, head.ProcessID-1) // - 1 because timeline contains index of processes from original Processes slice

		head.BurstDuration -= timeQuantum

		if head.BurstDuration > 0 { // process not finished, return to back of queue
			queue = append(queue, head)
		}
		currentTime++
	}

	totalWait, totalTurnaround, lastCompletion = getValues(timeline, processes, &schedule, &gantt)

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

func getValues(timeline []int64, processes []Process, schedule *[][]string, gantt *[]TimeSlice) (totalWait float64, totalTurnaround float64, lastCompletion float64) {
	var (
		start       int64
		end         int64
		waitingTime int64
		turnaround  int64
	)
	// tracks how much time each process is run since it started
	processRunTime := 0

	arrivalTimes := make([]int64, 0)
	// extract arrival times values
	for i := range processes {
		arrivalTimes = append(arrivalTimes, processes[i].ArrivalTime)
	}

	// traverse timeline and obtain start, end, waiting values
	for i := 1; i < len(timeline); i++ {
		processRunTime++
		if timeline[i] != timeline[i-1] { // if current element is different from previous, then a process change occurred
			start = int64(i - processRunTime) // start = current time - amount of time the process has been running
			end = int64(i)                    // end = current time

			// check waiting times
			// if processes[timeline[i-1]].ArrivalTime == 0 { // first process does not wait
			waitingTime = start - arrivalTimes[timeline[i-1]]
			totalWait += float64(waitingTime)

			// process was stopped to execute another process with lower burst dur, obtain execution stop time to calculate future waiting times
			if int64(processRunTime) != processes[timeline[i-1]].BurstDuration {
				arrivalTimes[timeline[i-1]] = int64(i) // update "arrival time" to be time where processes was preempted
			}

			turnaround = (end - start) + waitingTime
			totalTurnaround += float64(turnaround)
			completion := end

			*schedule = append(*schedule, []string{
				fmt.Sprint(processes[timeline[i-1]].ProcessID),
				fmt.Sprint(processes[timeline[i-1]].Priority),
				fmt.Sprint(processRunTime),
				fmt.Sprint(processes[timeline[i-1]].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			})

			*gantt = append(*gantt, TimeSlice{ // add to gantt schedule
				PID:   processes[timeline[i-1]].ProcessID, // process index is the timeline index - 1 to add processs that just finished
				Start: start,
				Stop:  end,
			})
			processRunTime = 0 // reset processRunTime time
		}
		if i == len(timeline)-1 { // last process to run
			start = int64(i - processRunTime)
			end = int64(i + 1)

			waitingTime = start - arrivalTimes[timeline[i-1]]
			totalWait += float64(waitingTime)

			turnaround = (end - start) + waitingTime
			totalTurnaround += float64(turnaround)
			completion := end
			lastCompletion = float64(completion)

			*schedule = append(*schedule, []string{
				fmt.Sprint(processes[timeline[i-1]].ProcessID),
				fmt.Sprint(processes[timeline[i-1]].Priority),
				fmt.Sprint(processRunTime + 1), // plus 1 because loop ends one time early
				fmt.Sprint(processes[timeline[i-1]].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			})

			*gantt = append(*gantt, TimeSlice{
				PID:   processes[timeline[i]].ProcessID, // process index is current timeline index
				Start: start,
				Stop:  end,
			})
			processRunTime = 0 // reset processRunTime time
		}
	}
	return totalWait, totalTurnaround, lastCompletion
}

//endregion
