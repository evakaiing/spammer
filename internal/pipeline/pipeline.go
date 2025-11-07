package pipeline

import (
	"fmt"
	"sort"
	"sync"

	"gitlab.vk-golang.ru/vk-golang/hw2/internal/service"
)

type Command func(in, out chan interface{})

func RunPipeline(cmds ...Command) {
	wg := &sync.WaitGroup{}
	channels := make([]chan interface{}, len(cmds)+1)
	for i := range channels {
		channels[i] = make(chan interface{})
	}

	for i, command := range cmds {
		wg.Add(1)
		go func(command Command, in, out chan interface{}) {
			defer wg.Done()
			command(in, out)
			close(out)
		}(command, channels[i], channels[i+1])
	}
	close(channels[0])
	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	users := make(map[string]bool)

	for email := range in {
		wg.Add(1)
		go func(email string) {
			defer wg.Done()
			user := service.GetUser(email)
			mu.Lock()
			if !users[user.Email] {
				users[user.Email] = true
				out <- user
			}
			mu.Unlock()
		}(email.(string))
	}
	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	users := make([]service.User, 0, service.GetMessagesMaxUsersBatch)
	wg := &sync.WaitGroup{}

	flush := func(batch []service.User) {
		if len(batch) == 0 {
			return
		}
		wg.Add(1)
		go func(batch []service.User) {
			defer wg.Done()
			messages, err := service.GetMessages(batch...)
			if err != nil {
				return
			}
			for _, msg := range messages {
				out <- msg
			}
		}(batch)
	}

	for user := range in {
		users = append(users, user.(service.User))
		if len(users) == service.GetMessagesMaxUsersBatch {
			batch := make([]service.User, len(users))
			copy(batch, users)
			users = users[:0]
			flush(batch)
		}
	}
	flush(users)
	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	jobs := make(chan interface{})
	wg := &sync.WaitGroup{}

	for i := 0; i < service.HasSpamMaxAsyncRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				res, err := service.HasSpam(id.(service.MsgID))
				if err != nil {
					continue
				}
				out <- service.MsgData{ID: id.(service.MsgID), HasSpam: res}
			}
		}()
	}

	for msg := range in {
		jobs <- msg
	}
	close(jobs)
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	results := []service.MsgData{}
	for msgData := range in {
		results = append(results, msgData.(service.MsgData))
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].HasSpam == results[j].HasSpam {
			return results[i].ID < results[j].ID
		}
		return results[i].HasSpam && !results[j].HasSpam
	})

	for _, msg := range results {
		out <- fmt.Sprintf("%t %d", msg.HasSpam, msg.ID)
	}
}
