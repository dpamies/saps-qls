package main

type QueryData struct {
	user  int
	query string
	num   int
	i     int
}

type Node struct {
	depth        int              `default:"0"`
	parent       *Node            `default:nil`
	tree         map[string]*Node `default:"{}"`
	users        []int            `default:"[]"`
	queries      []QueryData      `default:"[]"`
	branch_users map[int]Assemble `default:"{}"`
}

type Assemble struct {
	count int
	nodes map[*Node]int
}

