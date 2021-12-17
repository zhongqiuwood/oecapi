package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type task interface {
	part1()
	part2()
	id() int
}

type taskImp struct {
	block int
	idx int
	part1Elapse int
	wg *sync.WaitGroup
}

var taskChan chan task = make(chan task, 10000)
var sortChan chan task = make(chan task, 10000)
var abciChan chan task = make(chan task, 10000)
var expectedId int = 0

func part1Routine()  {
	for t := range taskChan {
		t.part1()
		sortChan <- t
	}
}


func sortRoutine()  {
	var taskMap = make(map[int]task)
	for t := range sortChan {
		if t.id() == expectedId {
			abciChan <- t
			expectedId++
			for {
				if next, ok := taskMap[expectedId]; ok {
					abciChan <- next
					delete(taskMap, expectedId)
					expectedId++
				} else {
					break
				}
			}
		} else {
			taskMap[t.id()] = t
		}
	}
}

func abciRoutine()  {
	for t := range abciChan {
		t.part2()
	}
}

func newTask(block, id int, wg *sync.WaitGroup) *taskImp {
	t := &taskImp{
		block: block,
		idx: id,
		part1Elapse: id%6,
		wg: wg,
	}
	return t
}

func (t *taskImp) id() int {
	return t.idx
}

func (t *taskImp) part1() {
	time.Sleep(time.Second*time.Duration(t.part1Elapse))
}

func (t *taskImp) part2() {
	fmt.Printf("block%d, task[%d]: part2\n", t.block, t.idx)
	t.wg.Done()
}

func initRoutine()  {
	part1RoutineNum := 30
	for i := 0; i < part1RoutineNum; i++ {
		go part1Routine()
	}
	go sortRoutine()
	go abciRoutine()
}

func run(block int, taskNum int)  {
	expectedId = 0
	var wg sync.WaitGroup
	wg.Add(taskNum)
	for i := 0; i < taskNum; i++ {
		task := newTask(block, i, &wg)
		taskChan <- task
	}
	wg.Wait()
}

func TestTask(t *testing.T) {
	initRoutine()

	// block 1
	run(1, 50)

	// block 2
	run(2, 100)
}