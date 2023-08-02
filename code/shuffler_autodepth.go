package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

//##################################################################################
//# Anonimizes logs shuffling users and queries.
//# The original query is NEVER assigned to the original user
//##################################################################################

var INIT_K int = 3
var MAX_TREE_DEPTH int = 1 // Limit tree levels to MAX_TREE_DEPTH
var data_tree Node         // Main tree
var result_f *bufio.Writer // Output file
var inlog int = 0
var outlog int = 0

// Chooses a suitable query
func choose_query(node *Node) (*QueryData, int) {
	randomIndex := rand.Intn(len(node.queries))
	return &node.queries[randomIndex], randomIndex
}

// Removes given user of all branch_user sets from the given node to the root
func update_users(node *Node, user int) {
	for current := node; current.parent != nil; current = current.parent {
		assemble := current.branch_users[user]
		if assemble.count > 1 { //Remove user from node
			assemble.count--
			if assemble.nodes[node] > 1 {
				assemble.nodes[node]--
			} else {
				delete(assemble.nodes, node)
			}
			current.branch_users[user] = assemble
		} else {
			delete(current.branch_users, user)
		}
	}
}

// Update branch_users between two branches which interchanged one user and share a common ancestor
func update_users_branch(from *Node, to *Node, top *Node, user int) {
	var assemble Assemble
	for current := from; current != top; current = current.parent {
		assemble = current.branch_users[user]
		if assemble.count > 1 { //Remove user from node
			assemble.count--
			if assemble.nodes[from] > 1 {
				assemble.nodes[from]--
			} else {
				delete(assemble.nodes, from)
			}
			current.branch_users[user] = assemble
		} else {
			delete(current.branch_users, user)
		}
	}

	for current := to; current != top; current = current.parent {
		var ok bool
		if assemble, ok = current.branch_users[user]; ok {
			assemble.count++
			assemble.nodes[to]++
			current.branch_users[user] = assemble
		} else {
			current.branch_users[user] = Assemble{count: 1, nodes: map[*Node]int{to: 1}}
		}
	}

	//Update user's cache from TOP to the root
	for current := top; current.parent != nil; current = current.parent {
		assemble = current.branch_users[user] //Treiem el node antic
		if assemble.nodes[from] > 1 {
			assemble.nodes[from]--
		} else {
			delete(assemble.nodes, from)
		}

		assemble.nodes[to]++ //Add the new node
		current.branch_users[user] = assemble
	}
}

// Find a random user that's not 'notuser'
func randuser(notuser int, branch *map[int]Assemble) int {
	i := rand.Intn(len(*branch))
	var user int
	for user = range *branch { //Search a different user starting from a random point
		if i == 0 {
			if user != notuser {
				return user
			}
		} else {
			i--
		}
	}
	for user = range *branch { //If we cannot find the user, we continue searching from the beginning 
		if user != notuser {
			return user
		}
	}
	return user //To avoid compiler warnings. Execution will never reach this point
}

func choose(validcat *Node) (*Node, int, *Node, QueryData) {
	// Pick a random element from branch_users
	i := rand.Intn(len((*validcat).branch_users))
	var ass Assemble
	for _, ass = range (*validcat).branch_users {
		if i == 0 {
			break
		}
		i--
	}
	// node_query: Pick a random node for the query
	i = rand.Intn(len(ass.nodes))
	var node_query *Node
	for node_query = range ass.nodes {
		if i == 0 {
			break
		}
		i--
	}
	query_idx := rand.Intn(len(node_query.queries))
	query := node_query.queries[query_idx]

	// user: Pick a random user
	user := randuser(query.user, &(*validcat).branch_users)
	i = rand.Intn(len((*validcat).branch_users[user].nodes))
	var node_user *Node
	for node_user = range (*validcat).branch_users[user].nodes {
		if i == 0 {
			break
		}
		i--
	}

	rmuser := &node_user.users // Remove the used user from node_user
	if len(*rmuser) > 1 {
		i := 0
		for (*rmuser)[i] != user {
			i++
		}
		(*rmuser)[i] = (*rmuser)[len(*rmuser)-1]
	}
	*rmuser = (*rmuser)[:len(*rmuser)-1]
	update_users(node_user, user)

	rmquery := &node_query.queries // Remove the used query from cat_query
	if len(*rmquery) > 1 {
		(*rmquery)[query_idx] = (*rmquery)[len(*rmquery)-1]
	}
	*rmquery = (*rmquery)[:len(*rmquery)-1]

	return node_user, user, node_query, query
}

func branch_query(category string, validcat *Node) {
	node_user, user, node_query, query := choose(validcat)

	top_category := strings.Join(strings.Split(strings.TrimSpace(category), ": ")[:validcat.depth], ": ") 
	result_f.WriteString(fmt.Sprintf("%d\t%d\t%s\t%s\n", query.num, user, query.query, top_category))
	outlog++

	if node_query != node_user { //Move one user to cat_user, if m un usuari a la cat_user (if it wasn't already in the same category)
		index := rand.Intn(len(node_query.users))
		mvuser := node_query.users[index]
		node_user.users = append(node_user.users, mvuser)
		node_query.users = append(node_query.users[:index], node_query.users[index+1:]...) //Remove user from cat_query

		update_users_branch(node_query, node_user, validcat, mvuser)
	}

	// Try to get another match from the same category
	unique_users := map[int]int{} // Category's unique users
	for _, u := range node_user.users {
		unique_users[u]++
	}

	if len(unique_users) >= INIT_K {
		query_choice, query_idx := choose_query(node_user)
		choose_user(node_user, query_choice, query_idx, category)
	}
}

// ##
// # Chooses a suitable user
// ##
func choose_user(curcat *Node, query_choice *QueryData, query_idx int, category string) int {
	user_idx := rand.Intn(len(curcat.users))
	user := curcat.users[user_idx]
	for user == query_choice.user { //Find a different user
		user_idx = rand.Intn(len(curcat.users))
		user = curcat.users[user_idx]
	}

	result_f.WriteString(fmt.Sprintf("%d\t%d\t%s\t%s\n", query_choice.num, user, query_choice.query, category)) 
	outlog++
	curcat.users = append(curcat.users[:user_idx], curcat.users[user_idx+1:]...)         //Remove user
	curcat.queries = append(curcat.queries[:query_idx], curcat.queries[query_idx+1:]...) //Remove query
	update_users(curcat, user)

	return user
}

// ##################################################################################
// # Main Function
// ##################################################################################
func main() {
	argv := os.Args[1:]
	if len(argv) == 2 {
		INIT_K, _ = strconv.Atoi(argv[0])
		INIT_K++
		MAX_TREE_DEPTH, _ = strconv.Atoi(argv[1])
	}

	data_tree = Node{users: []int{}, queries: []QueryData{}, tree: map[string]*Node{}, depth: 0}

	start := time.Now()

	f, _ := os.Open("../data/queries.txt")
	defer f.Close()
	result, _ := os.Create("../data/shuffle.txt")
	defer result.Close()

	result_f = bufio.NewWriter(result)
	defer result_f.Flush()

	lines := csv.NewReader(f)
	lines.Comma = '\t'

	for {
		line, err := lines.Read()
		if err == io.EOF {
			break
		}
		inlog++

		num, _ := strconv.Atoi(line[0])
		user, _ := strconv.Atoi(line[1])
		query := line[2] + "\t" + line[3]
		category := line[4]
		split_path := strings.Split(strings.TrimSpace(category), ": ")
		if len(split_path) > MAX_TREE_DEPTH {
			split_path = split_path[:MAX_TREE_DEPTH] //Limit tree levels to MAX_TREE_DEPTH
		}

		curcat := &data_tree
		var validcat *Node = nil
		depth := 1
		for _, name := range split_path { //For each node in the category path
			//Find the next node. If it doesn't exist we create it
			if v, ok := curcat.tree[name]; ok {
				curcat = v
			} else {
				curcat.tree[name] = &Node{branch_users: map[int]Assemble{}, users: []int{}, queries: []QueryData{}, tree: map[string]*Node{}, depth: depth, parent: curcat}
				curcat = curcat.tree[name]
			}

			depth++
			if _, ok := curcat.branch_users[user]; !ok { //If the category doesn't exist we create it
				curcat.branch_users[user] = Assemble{count: 0, nodes: map[*Node]int{}}
			}
			if len(curcat.branch_users) >= INIT_K { //Branch full
				validcat = curcat
			}
		}

		for current := curcat; current.parent != nil; current = current.parent {
			assemble := current.branch_users[user]
			assemble.count++
			assemble.nodes[curcat]++
			current.branch_users[user] = assemble
		}

		//Add user and query to the node
		curcat.users = append(curcat.users, user)
		curcat.queries = append(curcat.queries, QueryData{user: user, query: query, num: num, i: inlog}) 

		if validcat != nil { //Branch full
			unique_users := map[int]int{} //Find unique users of the current category
			for _, u := range curcat.users {
				unique_users[u]++
			}

			if len(unique_users) >= INIT_K { //Able to pair query with a different user in the same node
				query_choice, query_idx := choose_query(curcat)
				u := choose_user(curcat, query_choice, query_idx, category)

				if unique_users[u] == 1 { //If the selected user arity was 1, remove it from unique_users
					delete(unique_users, u)
				}
				if len(unique_users) >= INIT_K { // We are still over K, so let's try to take another pair
					query_choice, query_idx := choose_query(curcat)
					choose_user(curcat, query_choice, query_idx, category)
				}
			} else { //Able to pair query with a different user in the branch
				branch_query(category, validcat)
			}
		}
	}
}
