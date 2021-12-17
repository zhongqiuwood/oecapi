package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type TaskScheduler struct {
	expectedId int
	taskChan chan task
	sortChan chan task
	abciChan chan task
}

func newTaskScheduler(part1RoutineNum int) *TaskScheduler {
	ts := &TaskScheduler{}
	ts.taskChan = make(chan task, 10000)
	ts.sortChan = make(chan task, 10000)
	ts.abciChan = make(chan task, 10000)

	for i := 0; i < part1RoutineNum; i++ {
		go ts.part1Routine()
	}
	go ts.sortRoutine()
	go ts.abciRoutine()
	return ts
}

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


func (ts *TaskScheduler) part1Routine()  {
	for t := range ts.taskChan {
		t.part1()
		ts.sortChan <- t
	}
}


func (ts *TaskScheduler) sortRoutine()  {
	var taskMap = make(map[int]task)
	for t := range ts.sortChan {
		if t.id() == ts.expectedId {
			ts.abciChan <- t
			ts.expectedId++
			for {
				if next, ok := taskMap[ts.expectedId]; ok {
					ts.abciChan <- next
					delete(taskMap, ts.expectedId)
					ts.expectedId++
				} else {
					break
				}
			}
		} else {
			taskMap[t.id()] = t
		}
	}
}

func (ts *TaskScheduler) abciRoutine()  {
	for t := range ts.abciChan {
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


func (ts *TaskScheduler) run(block int, taskNum int)  {
	ts.expectedId = 0
	var wg sync.WaitGroup
	wg.Add(taskNum)
	for i := 0; i < taskNum; i++ {
		task := newTask(block, i, &wg)
		ts.taskChan <- task
	}
	wg.Wait()
}

func TestTaskScheduler(t *testing.T) {
	ts := newTaskScheduler(30)

	// block 1
	ts.run(1, 50)

	// block 2
	ts.run(2, 100)
}