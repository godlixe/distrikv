package cli

// func Start() {
// 	sstManager, err := storage.NewSSTManager()
// 	if err != nil {
// 		panic(err)
// 	}

// 	compactorManager := storage.NewCompactorManager(sstManager)

// 	compactorManager.StartCompactors(context.Background())

// 	store := storage.NewStore(sstManager)
// 	reader := bufio.NewReader(os.Stdin)

// 	fmt.Println("In-memory KV store. Commands: set key value | get key | delete key | exit")

// 	for {
// 		fmt.Print("> ")
// 		line, _ := reader.ReadString('\n')
// 		line = strings.TrimSpace(line)

// 		if line == "exit" {
// 			break
// 		}

// 		parts := strings.Fields(line)
// 		if len(parts) == 0 {
// 			continue
// 		}

// 		switch parts[0] {
// 		case "set":
// 			if len(parts) < 3 {
// 				fmt.Println("Usage: set key value")
// 				continue
// 			}
// 			key := parts[1]
// 			value := strings.Join(parts[2:], " ")
// 			store.Set(key, value)
// 			fmt.Println("OK")

// 		case "get":
// 			if len(parts) != 2 {
// 				fmt.Println("Usage: get key")
// 				continue
// 			}
// 			val, err := store.Get(parts[1])
// 			if err != nil {
// 				fmt.Println("Not found")
// 			} else {
// 				fmt.Println(string(val.Value))
// 			}

// 		case "delete":
// 			if len(parts) != 2 {
// 				fmt.Println("Usage: delete key")
// 				continue
// 			}
// 			store.Delete(parts[1])
// 			fmt.Println("Deleted")

// 		default:
// 			fmt.Println("Unknown command:", parts[0])
// 		}
// 	}

// }
