The hacker news api provides an [endpoint](https://hacker-news.firebaseio.com/v0/maxitem) that displays the max item id. You can walk backwards from that id to pull down all [items](https://github.com/HackerNews/API#items).

Options:
```sh
$ go run hacker_news.go -h
```