// Package main реализует конвейерную обработку писем на наличие спама
//
// Программа использует pipeline для последовательной обработки 
// электронных писем: получение пользователей, получений сообщений, проверка на спам
// и комбинирование результата
package main


import (
	"sort"
	"sync"
	"fmt"
)

// RunPipeline запускает последовательность команд в виде конвейера.
//
// Каждая команда выполняется в отдельной горутине, связана с соседними каналом.
//
// Параметры:
// 	- cmds - сmd
func RunPipeline(cmds ...cmd) {
	wg := &sync.WaitGroup{}
	channels := make([]chan interface{}, len(cmds) + 1)
	for i := range channels {
		channels[i] = make(chan interface{})
	}	
	for i, command := range cmds {
		wg.Add(1)
		go func(command cmd, in, out chan interface{}) {
			defer wg.Done()
			command(in, out);
			close(out)
		}(command, channels[i], channels[i + 1])
	}
	close(channels[0])
	wg.Wait()	
}


// SelectUsers получает пользователей по их email.
// 
// Параметры:
// 	- in - string
// 	- out - User
func SelectUsers(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	users := make(map[string]bool)

	for email := range in {
		wg.Add(1)
		go func(in, out chan interface{}) {
			defer wg.Done()
			user := GetUser(email.(string))
			// Map не потокобезопасна, требуется мьютекс для избежания race
			mu.Lock()
			_, isExist := users[user.Email]
			if !isExist {
				users[user.Email] = true
				out <- user
			}
			mu.Unlock()

		} (in, out)
	}
	wg.Wait()
}

// SelectMessages получает сообщения для пользователей батчами.
//
// Функция накапливает пользователей в пакеты размером GetMessagesMaxUsersBatch
// и параллельно запрашивает их сообщения. 
//
// Параметры:
//   - in - User
//   - out - MsgID
func SelectMessages(in, out chan interface{}) {
	users := make([]User, 0, GetMessagesMaxUsersBatch)
	wg := &sync.WaitGroup{}
	for user := range in {
		users = append(users, user.(User))
		if (len(users) == GetMessagesMaxUsersBatch) {
			batch := make([]User, len(users))
			copy(batch, users)
			users = make([]User, 0, GetMessagesMaxUsersBatch)

			wg.Add(1)
			go func(batch []User) {
				defer wg.Done()
				messages, err := GetMessages(batch...)
				if err == nil {
					for _, msg := range messages {
						out <- msg
					}
				}
			}(batch)
		}

	}

	if (len(users) != 0) {
		wg.Add(1)
		go func(out chan interface{}) {
			defer wg.Done()
			messages, err := GetMessages(users...)
			if err == nil {
				for _, msg := range messages {
					out <- msg
				}
			}
		}(out)
	}

		
	wg.Wait()
}

// CheckSpam проверяет сообщения на наличие спама с использованием worker pool.
//
// Функция создает пул воркеров размером HasSpamMaxAsyncRequests для
// параллельной проверки сообщений. Это необходимо из-за ограничений
// антибрут-защиты сервиса HasSpam.
//
// Параметры:
//  - in - MsgID
// 	- out - MsgData
func CheckSpam(in, out chan interface{}) {
	jobs := make(chan interface{})
	wg := &sync.WaitGroup{}
	for i := 0; i < HasSpamMaxAsyncRequests; i++ {
		wg.Add(1)
		go func(jobs, out chan interface{}) {
			defer wg.Done()
			for id := range jobs {
				res, err := HasSpam(id.(MsgID))
				if (err == nil) {
					res := MsgData{
						ID: id.(MsgID),
						HasSpam: res,
					}
					out <- res
				}
			}
		} (jobs, out)
	}

	for msg := range in {
		jobs <- msg
	}
	close(jobs)

	wg.Wait()

}

// CombineResults объединяет результаты проверки и сортирует их.
//
// Параметры:
// in - MsgData
// out - string
func CombineResults(in, out chan interface{}) {
	// in - MsgData
	// out - string
	results := []MsgData{}
	for msgData := range in {
		results = append(results, msgData.(MsgData))
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
